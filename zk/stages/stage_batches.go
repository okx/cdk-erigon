package stages

import (
	"context"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"

	"github.com/gateway-fm/cdk-erigon-lib/kv"

	ethTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/erigon_db"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	txtype "github.com/ledgerwatch/erigon/zk/tx"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/log/v3"
)

const (
	preForkId7BlockGasLimit = 30_000_000
	forkId7BlockGasLimit    = 18446744073709551615 // 0xffffffffffffffff
	forkId8BlockGasLimit    = 1125899906842624     // 0x4000000000000
	HIGHEST_KNOWN_FORK      = 9
	newBlockTimeout         = 500
)

type ErigonDb interface {
	WriteHeader(batchNo *big.Int, stateRoot, txHash, parentHash common.Hash, coinbase common.Address, ts, gasLimit uint64) (*ethTypes.Header, error)
	WriteBody(batchNo *big.Int, headerHash common.Hash, txs []ethTypes.Transaction) error
}

type HermezDb interface {
	WriteForkId(batchNumber uint64, forkId uint64) error
	WriteForkIdBlockOnce(forkId, blockNum uint64) error
	WriteBlockBatch(l2BlockNumber uint64, batchNumber uint64) error
	WriteEffectiveGasPricePercentage(txHash common.Hash, effectiveGasPricePercentage uint8) error
	DeleteEffectiveGasPricePercentages(txHashes *[]common.Hash) error

	WriteStateRoot(l2BlockNumber uint64, rpcRoot common.Hash) error

	DeleteForkIds(fromBatchNum, toBatchNum uint64) error
	DeleteBlockBatches(fromBlockNum, toBlockNum uint64) error

	CheckGlobalExitRootWritten(ger common.Hash) (bool, error)
	WriteBlockGlobalExitRoot(l2BlockNo uint64, ger common.Hash) error
	WriteGlobalExitRoot(ger common.Hash) error
	DeleteBlockGlobalExitRoots(fromBlockNum, toBlockNum uint64) error
	DeleteGlobalExitRoots(l1BlockHashes *[]common.Hash) error

	WriteReusedL1InfoTreeIndex(l2BlockNo uint64) error
	WriteBlockL1BlockHash(l2BlockNo uint64, l1BlockHash common.Hash) error
	DeleteBlockL1BlockHashes(fromBlockNum, toBlockNum uint64) error
	WriteL1BlockHash(l1BlockHash common.Hash) error
	CheckL1BlockHashWritten(l1BlockHash common.Hash) (bool, error)
	DeleteL1BlockHashes(l1BlockHashes *[]common.Hash) error
	WriteGerForL1BlockHash(l1BlockHash, ger common.Hash) error
	DeleteL1BlockHashGers(l1BlockHashes *[]common.Hash) error
	WriteBatchGlobalExitRoot(batchNumber uint64, ger types.GerUpdate) error
	WriteIntermediateTxStateRoot(l2BlockNumber uint64, txHash common.Hash, rpcRoot common.Hash) error
	WriteBlockL1InfoTreeIndex(blockNumber uint64, l1Index uint64) error
}

type DatastreamClient interface {
	ReadAllEntriesToChannel(bookmark *types.Bookmark) error
	GetL2BlockChan() chan types.FullL2Block
	GetErrChan() chan error
	GetGerUpdatesChan() chan types.GerUpdate
	GetLastWrittenTimeAtomic() *atomic.Int64
	GetStreamingAtomic() *atomic.Bool
}

type BatchesCfg struct {
	db                  kv.RwDB
	blockRoutineStarted bool
	dsClient            DatastreamClient
	zkCfg               *ethconfig.Zk
}

func StageBatchesCfg(db kv.RwDB, dsClient DatastreamClient, zkCfg *ethconfig.Zk) BatchesCfg {
	return BatchesCfg{
		db:                  db,
		blockRoutineStarted: false,
		dsClient:            dsClient,
		zkCfg:               zkCfg,
	}
}

var emptyHash = common.Hash{0}

func SpawnStageBatches(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg BatchesCfg,
	firstCycle bool,
	quiet bool,
) error {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting batches stage", logPrefix))
	if sequencer.IsSequencer() {
		log.Info(fmt.Sprintf("[%s] skipping -- sequencer", logPrefix))
		return nil
	}
	defer log.Info(fmt.Sprintf("[%s] Finished Batches stage", logPrefix))

	if tx == nil {
		log.Debug(fmt.Sprintf("[%s] batches: no tx provided, creating a new one", logPrefix))
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return fmt.Errorf("failed to open tx, %w", err)
		}
		defer tx.Rollback()
	}

	eriDb := erigon_db.NewErigonDb(tx)
	hermezDb := hermez_db.NewHermezDb(tx)

	batchesProgress, err := stages.GetStageProgress(tx, stages.Batches)
	if err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	// get batch for batches progress
	batchesProgressBatch, err := hermezDb.GetBatchNoByL2Block(batchesProgress)
	if err != nil {
		return fmt.Errorf("get batch no by l2 block error: %v", err)
	}

	//// BISECT ////
	if cfg.zkCfg.DebugLimit > 0 && batchesProgress > cfg.zkCfg.DebugLimit {
		return nil
	}

	highestVerifiedBatch, err := stages.GetStageProgress(tx, stages.L1VerificationsBatchNo)
	if err != nil {
		return fmt.Errorf("could not retrieve l1 verifications batch no progress")
	}

	startSyncTime := time.Now()
	// start routine to download blocks and push them in a channel
	if !cfg.dsClient.GetStreamingAtomic().Load() {
		log.Info(fmt.Sprintf("[%s] Starting stream", logPrefix), "startBlock", batchesProgress)
		go func() {
			log.Info(fmt.Sprintf("[%s] Started downloading L2Blocks routine", logPrefix))
			defer log.Info(fmt.Sprintf("[%s] Finished downloading L2Blocks routine", logPrefix))

			// this will download all blocks from datastream and push them in a channel
			// if no error, break, else continue trying to get them
			// Create bookmark
			bookmark := types.NewL2BlockBookmark(batchesProgress)
			cfg.dsClient.ReadAllEntriesToChannel(bookmark)
		}()
	}

	// start a routine to print blocks written progress
	progressChan, stopProgressPrinter := zk.ProgressPrinterWithoutTotal(fmt.Sprintf("[%s] Downloaded blocks from datastream progress", logPrefix))
	defer stopProgressPrinter()

	lastBlockHeight := batchesProgress
	highestSeenBatchNo := batchesProgressBatch
	endLoop := false
	blocksWritten := uint64(0)
	highestHashableL2BlockNo := uint64(0)

	highestL1InfoTreeIndex, err := stages.GetStageProgress(tx, stages.HighestUsedL1InfoIndex)
	if err != nil {
		return fmt.Errorf("failed to get highest used l1 info index, %w", err)
	}

	lastForkId64, err := stages.GetStageProgress(tx, stages.ForkId)
	lastForkId := uint16(lastForkId64)
	if err != nil {
		return fmt.Errorf("failed to get last fork id, %w", err)
	}

	stageExecProgress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return fmt.Errorf("failed to get stage exec progress, %w", err)
	}

	lastHash := emptyHash
	atLeastOneBlockWritten := false
	startTime := time.Now()

	log.Info(fmt.Sprintf("[%s] Reading blocks from the datastream.", logPrefix))

	l2BlockChan := cfg.dsClient.GetL2BlockChan()
	gerUpdateChan := cfg.dsClient.GetGerUpdatesChan()
	lastWrittenTimeAtomic := cfg.dsClient.GetLastWrittenTimeAtomic()
	streamingAtomic := cfg.dsClient.GetStreamingAtomic()
	errChan := cfg.dsClient.GetErrChan()

LOOP:
	for {
		// get block
		// if no blocks available should block
		// if download routine finished, should continue to read from channel until it's empty
		// if both download routine stopped and channel empty - stop loop
		select {
		case l2Block := <-l2BlockChan:
			if cfg.zkCfg.SyncLimit > 0 && l2Block.L2BlockNumber >= cfg.zkCfg.SyncLimit {
				// stop the node going into a crazy loop
				time.Sleep(2 * time.Second)
				break LOOP
			}

			atLeastOneBlockWritten = true
			// skip if we already have this block
			if l2Block.L2BlockNumber < lastBlockHeight+1 {
				continue
			}

			// check for sequential block numbers
			if l2Block.L2BlockNumber != lastBlockHeight+1 {
				return fmt.Errorf("block number is not sequential, expected %d, got %d", lastBlockHeight+1, l2Block.L2BlockNumber)
			}

			// batch boundary - record the highest hashable block number (last block in last full batch)
			if l2Block.BatchNumber > highestSeenBatchNo {
				highestHashableL2BlockNo = l2Block.L2BlockNumber - 1
			}
			highestSeenBatchNo = l2Block.BatchNumber

			if l2Block.ForkId > HIGHEST_KNOWN_FORK {
				message := fmt.Sprintf("unsupported fork id %v received from the data stream", l2Block.ForkId)
				panic(message)
			}

			/////// DEBUG BISECTION ///////
			// exit stage when debug bisection flags set and we're at the limit block
			if cfg.zkCfg.DebugLimit > 0 && l2Block.L2BlockNumber > cfg.zkCfg.DebugLimit {
				fmt.Printf("[%s] Debug limit reached, stopping stage\n", logPrefix)
				endLoop = true
			}

			// if we're above StepAfter, and we're at a step, move the stages on
			if cfg.zkCfg.DebugStep > 0 && cfg.zkCfg.DebugStepAfter > 0 && l2Block.L2BlockNumber > cfg.zkCfg.DebugStepAfter {
				fmt.Printf("[%s] Debug step after reached, continuing stage\n", logPrefix)
				if l2Block.L2BlockNumber%cfg.zkCfg.DebugStep == 0 {
					fmt.Printf("[%s] Debug step reached, stopping stage\n", logPrefix)
					endLoop = true
				}
			}
			/////// END DEBUG BISECTION ///////

			// update forkid
			if l2Block.ForkId > lastForkId {
				log.Info(fmt.Sprintf("[%s] Updated fork id, last fork id %d, new fork id:%d, block num:%d", logPrefix, lastForkId, l2Block.ForkId, l2Block.L2BlockNumber))
				lastForkId = l2Block.ForkId
				err = hermezDb.WriteForkId(l2Block.BatchNumber, uint64(l2Block.ForkId))
				if err != nil {
					return fmt.Errorf("write fork id error: %v", err)
				}

				if err := hermezDb.WriteForkIdBlockOnce(uint64(l2Block.ForkId), l2Block.L2BlockNumber); err != nil {
					return fmt.Errorf("write fork id block once error: %v", err)
				}
			}

			// store our finalized state if this batch matches the highest verified batch number on the L1
			if l2Block.BatchNumber == highestVerifiedBatch {
				rawdb.WriteForkchoiceFinalized(tx, l2Block.L2Blockhash)
			}

			if lastHash != emptyHash {
				l2Block.ParentHash = lastHash
			} else {
				// first block in the loop so read the parent hash
				previousHash, err := eriDb.ReadCanonicalHash(l2Block.L2BlockNumber - 1)
				if err != nil {
					return fmt.Errorf("failed to get genesis header: %v", err)
				}
				l2Block.ParentHash = previousHash
			}

			if err := writeL2Block(eriDb, hermezDb, &l2Block, highestL1InfoTreeIndex); err != nil {
				return fmt.Errorf("writeL2Block error: %v", err)
			}

			// make sure to capture the l1 info tree index changes, so we can store progress
			if uint64(l2Block.L1InfoTreeIndex) > highestL1InfoTreeIndex {
				highestL1InfoTreeIndex = uint64(l2Block.L1InfoTreeIndex)
			}

			lastHash = l2Block.L2Blockhash

			lastBlockHeight = l2Block.L2BlockNumber
			blocksWritten++
			progressChan <- blocksWritten

			if endLoop && cfg.zkCfg.DebugLimit > 0 {
				break LOOP
			}
		case gerUpdate := <-gerUpdateChan:
			if gerUpdate.GlobalExitRoot == emptyHash {
				log.Warn(fmt.Sprintf("[%s] Skipping GER update with empty root", logPrefix))
				break
			}

			// NB: we won't get these post Etrog (fork id 7)
			if err := hermezDb.WriteBatchGlobalExitRoot(gerUpdate.BatchNumber, gerUpdate); err != nil {
				return fmt.Errorf("write batch global exit root error: %v", err)
			}
		case err := <-errChan:
			if err != nil {
				return fmt.Errorf("l2blocks download routine error: %v", err)
			}
		default:
			// wait at least one block to be written, before continuing
			// or if stage_exec is ahead - don't wait here, but rather continue so exec catches up
			if atLeastOneBlockWritten || stageExecProgress < lastBlockHeight {
				// if no blocks available should and time since last block written is > 500ms
				// consider that we are at the tip and blocks come in the datastream as they are produced
				// stop the current iteration of the stage
				lastWrittenTs := lastWrittenTimeAtomic.Load()
				timePassedAfterlastBlock := time.Since(time.Unix(0, lastWrittenTs))
				if streamingAtomic.Load() && timePassedAfterlastBlock.Milliseconds() > newBlockTimeout {
					log.Info(fmt.Sprintf("[%s] No new blocks in %d miliseconds. Ending the stage.", logPrefix, timePassedAfterlastBlock.Milliseconds()), "lastBlockHeight", lastBlockHeight)
					endLoop = true
				}
			} else {
				timePassedAfterlastBlock := time.Since(startTime)
				if timePassedAfterlastBlock.Seconds() > 10 {
					// if the connection ropped, continue with next stages while it tries to reconnect
					// otherwise it will get stuck in "waiting for at least one block to be written" loop
					// if !streamingAtomic.Load() {
					// 	endLoop = true
					// 	break
					// }

					log.Info(fmt.Sprintf("[%s] Waiting for at least one new block.", logPrefix))
					startTime = time.Now()
				}
			}
		}

		if endLoop {
			log.Info(fmt.Sprintf("[%s] Total blocks read: %d", logPrefix, blocksWritten))
			break
		}
	}

	if lastBlockHeight == batchesProgress {
		return nil
	}

	// store the highest hashable block number
	if err := stages.SaveStageProgress(tx, stages.HighestHashableL2BlockNo, highestHashableL2BlockNo); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	if err = stages.SaveStageProgress(tx, stages.HighestSeenBatchNumber, highestSeenBatchNo); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	// store the highest seen forkid
	if err := stages.SaveStageProgress(tx, stages.ForkId, uint64(lastForkId)); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	if err := stages.SaveStageProgress(tx, stages.HighestUsedL1InfoIndex, uint64(highestL1InfoTreeIndex)); err != nil {
		return err
	}

	// save the latest verified batch number as well just in case this node is upgraded
	// to a sequencer in the future
	if err := stages.SaveStageProgress(tx, stages.SequenceExecutorVerify, highestSeenBatchNo); err != nil {
		return fmt.Errorf("save stage progress error: %w", err)
	}

	// stop printing blocks written progress routine
	elapsed := time.Since(startSyncTime)
	log.Info(fmt.Sprintf("[%s] Finished writing blocks", logPrefix), "blocksWritten", blocksWritten, "elapsed", elapsed)

	log.Info(fmt.Sprintf("[%s] Saving stage progress", logPrefix), "lastBlockHeight", lastBlockHeight)
	if err := stages.SaveStageProgress(tx, stages.Batches, lastBlockHeight); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	if firstCycle {
		log.Debug(fmt.Sprintf("[%s] batches: first cycle, committing tx", logPrefix))
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit tx, %w", err)
		}
	}

	return nil
}

func UnwindBatchesStage(u *stagedsync.UnwindState, tx kv.RwTx, cfg BatchesCfg, ctx context.Context) (err error) {
	logPrefix := u.LogPrefix()

	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	fromBlock := u.UnwindPoint
	toBlock := u.CurrentBlockNumber
	log.Info(fmt.Sprintf("[%s] Unwinding batches stage from block number", logPrefix), "fromBlock", fromBlock, "toBlock", toBlock)
	defer log.Info(fmt.Sprintf("[%s] Unwinding batches complete", logPrefix))

	eriDb := erigon_db.NewErigonDb(tx)
	hermezDb := hermez_db.NewHermezDb(tx)

	//////////////////////////////////
	// delete batch connected stuff //
	//////////////////////////////////
	highestVerifiedBatch, err := stages.GetStageProgress(tx, stages.L1VerificationsBatchNo)
	if err != nil {
		return fmt.Errorf("could not retrieve l1 verifications batch no progress")
	}

	fromBatchPrev, err := hermezDb.GetBatchNoByL2Block(fromBlock - 1)
	if err != nil {
		return fmt.Errorf("get batch no by l2 block error: %v", err)
	}
	fromBatch, err := hermezDb.GetBatchNoByL2Block(fromBlock)
	if err != nil {
		return fmt.Errorf("get fromBatch no by l2 block error: %v", err)
	}
	toBatch, err := hermezDb.GetBatchNoByL2Block(toBlock)
	if err != nil {
		return fmt.Errorf("get toBatch no by l2 block error: %v", err)
	}

	// if previous block has different batch, delete the "fromBlock" one
	// since it is written first in this block
	// otherwise don't delete it and start from the next batch
	if fromBatchPrev == fromBatch && fromBatch != 0 {
		fromBatch++
	}

	if fromBatch <= toBatch {
		if err := hermezDb.DeleteForkIds(fromBatch, toBatch); err != nil {
			return fmt.Errorf("delete fork ids error: %v", err)
		}
		if err := hermezDb.DeleteBatchGlobalExitRoots(fromBatch); err != nil {
			return fmt.Errorf("delete batch global exit roots error: %v", err)
		}
	}

	if highestVerifiedBatch >= fromBatch {
		if err := rawdb.DeleteForkchoiceFinalized(tx); err != nil {
			return fmt.Errorf("delete forkchoice finalized error: %v", err)
		}
	}
	/////////////////////////////////////////
	// finish delete batch connected stuff //
	/////////////////////////////////////////

	//get transactions before deleting them so we can delete stuff connected to them
	transactions, err := eriDb.GetBodyTransactions(fromBlock, toBlock)
	if err != nil {
		return fmt.Errorf("get body transactions error: %v", err)
	}
	transactionHashes := make([]common.Hash, 0, len(*transactions))
	for _, tx := range *transactions {
		transactionHashes = append(transactionHashes, tx.Hash())
	}

	if err := hermezDb.DeleteEffectiveGasPricePercentages(&transactionHashes); err != nil {
		return fmt.Errorf("delete effective gas price percentages error: %v", err)
	}
	if err := hermezDb.DeleteStateRoots(fromBlock, toBlock); err != nil {
		return fmt.Errorf("delete state roots error: %v", err)
	}
	if err := hermezDb.DeleteIntermediateTxStateRoots(fromBlock, toBlock); err != nil {
		return fmt.Errorf("delete intermediate tx state roots error: %v", err)
	}
	if err := eriDb.DeleteHeaders(fromBlock); err != nil {
		return fmt.Errorf("delete headers error: %v", err)
	}
	if err := eriDb.DeleteBodies(fromBlock); err != nil {
		return fmt.Errorf("delete bodies error: %v", err)
	}
	if err := hermezDb.DeleteBlockBatches(fromBlock, toBlock); err != nil {
		return fmt.Errorf("delete block batches error: %v", err)
	}
	if err := hermezDb.DeleteForkIdBlock(fromBlock, toBlock); err != nil {
		return fmt.Errorf("delete fork id block error: %v", err)
	}

	//////////////////////////////////////////////////////
	// get gers and l1BlockHashes before deleting them				    //
	// so we can delete them in the other table as well //
	//////////////////////////////////////////////////////
	gers, err := hermezDb.GetBlockGlobalExitRoots(fromBlock, toBlock)
	if err != nil {
		return fmt.Errorf("get block global exit roots error: %v", err)
	}

	l1BlockHashes, err := hermezDb.GetBlockL1BlockHashes(fromBlock, toBlock)
	if err != nil {
		return fmt.Errorf("get block l1 block hashes error: %v", err)
	}

	if err := hermezDb.DeleteGlobalExitRoots(&gers); err != nil {
		return fmt.Errorf("delete global exit roots error: %v", err)
	}

	if err := hermezDb.DeleteBlockGlobalExitRoots(fromBlock, toBlock); err != nil {
		return fmt.Errorf("delete block global exit roots error: %v", err)
	}

	if err := hermezDb.DeleteL1BlockHashes(&l1BlockHashes); err != nil {
		return fmt.Errorf("delete l1 block hashes error: %v", err)
	}

	if err := hermezDb.DeleteL1BlockHashGers(&l1BlockHashes); err != nil {
		return fmt.Errorf("delete l1 block hash gers error: %v", err)
	}

	if err := hermezDb.DeleteBlockL1BlockHashes(fromBlock, toBlock); err != nil {
		return fmt.Errorf("delete block l1 block hashes error: %v", err)
	}

	if err := hermezDb.DeleteBlockL1InfoTreeIndexes(fromBlock, toBlock); err != nil {
		return fmt.Errorf("delete block l1 block hashes error: %v", err)
	}
	///////////////////////////////////////////////////////

	log.Info(fmt.Sprintf("[%s] Deleted headers, bodies, forkIds and blockBatches.", logPrefix))
	log.Info(fmt.Sprintf("[%s] Saving stage progress", logPrefix), "fromBlock", fromBlock)

	stageprogress := uint64(0)
	if fromBlock > 1 {
		stageprogress = fromBlock - 1
	}
	if err := stages.SaveStageProgress(tx, stages.Batches, stageprogress); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	/////////////////////////////////////////////
	// store the highest hashable block number //
	/////////////////////////////////////////////
	// iterate until a block with lower batch number is found
	// this is the last block of the previous batch and the highest hashable block for verifications
	highestHashableL2BlockNo := uint64(fromBlock)
	for i := fromBlock; i > 0; i-- {
		batchNo, err := hermezDb.GetBatchNoByL2Block(i)
		if err != nil {
			return fmt.Errorf("get batch no by l2 block error: %v", err)
		}
		if batchNo == fromBatch-1 {
			highestHashableL2BlockNo = uint64(i)
			break
		}
	}

	if err := stages.SaveStageProgress(tx, stages.HighestHashableL2BlockNo, highestHashableL2BlockNo); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}
	/////////////////////////////////////////////////////
	// finish storing the highest hashable block number//
	/////////////////////////////////////////////////////

	//////////////////////////////////
	// store the highest seen forkid//
	//////////////////////////////////
	forkId, err := hermezDb.GetForkId(fromBatchPrev)
	if err != nil {
		return fmt.Errorf("get fork id error: %v", err)
	}
	if err := stages.SaveStageProgress(tx, stages.ForkId, forkId); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}
	/////////////////////////////////////////
	// finish store the highest seen forkid//
	/////////////////////////////////////////

	if err = stages.SaveStageProgress(tx, stages.HighestSeenBatchNumber, fromBatchPrev); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	if err := u.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func PruneBatchesStage(s *stagedsync.PruneState, tx kv.RwTx, cfg BatchesCfg, ctx context.Context) (err error) {
	logPrefix := s.LogPrefix()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	log.Info(fmt.Sprintf("[%s] Pruning barches...", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Unwinding batches complete", logPrefix))

	eriDb := erigon_db.NewErigonDb(tx)
	hermezDb := hermez_db.NewHermezDb(tx)

	toBlock, err := stages.GetStageProgress(tx, stages.Batches)
	if err != nil {
		return fmt.Errorf("get stage datastream progress error: %v", err)
	}

	eriDb.DeleteBodies(0)
	eriDb.DeleteHeaders(0)

	hermezDb.DeleteForkIds(0, toBlock)
	hermezDb.DeleteBlockBatches(0, toBlock)
	hermezDb.DeleteBlockGlobalExitRoots(0, toBlock)

	log.Info(fmt.Sprintf("[%s] Deleted headers, bodies, forkIds and blockBatches.", logPrefix))
	log.Info(fmt.Sprintf("[%s] Saving stage progress", logPrefix), "stageProgress", 0)
	if err := stages.SaveStageProgress(tx, stages.Batches, 0); err != nil {
		return fmt.Errorf("save stage progress error: %v", err)
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func getGasLimit(forkId uint16) uint64 {
	if forkId >= 8 {
		return forkId8BlockGasLimit
	}

	// [hack] the rpc returns forkid8 value, but forkid7 is used in execution
	if forkId == 7 {
		return forkId8BlockGasLimit
		// return forkId7BlockGasLimit
	}

	return preForkId7BlockGasLimit
}

// writeL2Block writes L2Block to ErigonDb and HermezDb
// writes header, body, forkId and blockBatch
func writeL2Block(eriDb ErigonDb, hermezDb HermezDb, l2Block *types.FullL2Block, highestL1InfoTreeIndex uint64) error {
	bn := new(big.Int).SetUint64(l2Block.L2BlockNumber)
	txs := make([]ethTypes.Transaction, 0, len(l2Block.L2Txs))
	for _, transaction := range l2Block.L2Txs {
		ltx, _, err := txtype.DecodeTx(transaction.Encoded, transaction.EffectiveGasPricePercentage, l2Block.ForkId)
		if err != nil {
			return fmt.Errorf("decode tx error: %v", err)
		}
		txs = append(txs, ltx)

		if err := hermezDb.WriteEffectiveGasPricePercentage(ltx.Hash(), transaction.EffectiveGasPricePercentage); err != nil {
			return fmt.Errorf("write effective gas price percentage error: %v", err)
		}

		if err := hermezDb.WriteStateRoot(l2Block.L2BlockNumber, transaction.StateRoot); err != nil {
			return fmt.Errorf("write rpc root error: %v", err)
		}

		if err := hermezDb.WriteIntermediateTxStateRoot(l2Block.L2BlockNumber, ltx.Hash(), transaction.StateRoot); err != nil {
			return fmt.Errorf("write rpc root error: %v", err)
		}
	}
	txCollection := ethTypes.Transactions(txs)
	txHash := ethTypes.DeriveSha(txCollection)

	gasLimit := getGasLimit(l2Block.ForkId)

	h, err := eriDb.WriteHeader(bn, l2Block.StateRoot, txHash, l2Block.ParentHash, l2Block.Coinbase, uint64(l2Block.Timestamp), gasLimit)
	if err != nil {
		return fmt.Errorf("write header error: %v", err)
	}

	if l2Block.GlobalExitRoot != emptyHash {
		gerWritten, err := hermezDb.CheckGlobalExitRootWritten(l2Block.GlobalExitRoot)
		if err != nil {
			return fmt.Errorf("get global exit root error: %v", err)
		}

		if !gerWritten {
			if err := hermezDb.WriteBlockGlobalExitRoot(l2Block.L2BlockNumber, l2Block.GlobalExitRoot); err != nil {
				return fmt.Errorf("write block global exit root error: %v", err)
			}

			if err := hermezDb.WriteGlobalExitRoot(l2Block.GlobalExitRoot); err != nil {
				return fmt.Errorf("write global exit root error: %v", err)
			}
		}
	}

	if l2Block.L1BlockHash != emptyHash {
		l1BlockHashWritten, err := hermezDb.CheckL1BlockHashWritten(l2Block.L1BlockHash)
		if err != nil {
			return fmt.Errorf("get global exit root error: %v", err)
		}

		if !l1BlockHashWritten {
			if err := hermezDb.WriteBlockL1BlockHash(l2Block.L2BlockNumber, l2Block.L1BlockHash); err != nil {
				return fmt.Errorf("write block global exit root error: %v", err)
			}

			if err := hermezDb.WriteL1BlockHash(l2Block.L1BlockHash); err != nil {
				return fmt.Errorf("write global exit root error: %v", err)
			}

			if l2Block.GlobalExitRoot != emptyHash {
				if err := hermezDb.WriteGerForL1BlockHash(l2Block.L1BlockHash, l2Block.GlobalExitRoot); err != nil {
					return fmt.Errorf("write ger for l1 block hash error: %v", err)
				}
			}
		}
	}

	if l2Block.L1InfoTreeIndex != 0 {
		if err = hermezDb.WriteBlockL1InfoTreeIndex(l2Block.L2BlockNumber, uint64(l2Block.L1InfoTreeIndex)); err != nil {
			return err
		}

		// if the info tree index of this block is lower than the highest we've seen
		// we need to write the GER and l1 block hash regardless of the logic above.
		// this can only happen in post etrog blocks, and we need the GER/L1 block hash
		// for the stream and also for the block info root to be correct
		if uint64(l2Block.L1InfoTreeIndex) <= highestL1InfoTreeIndex {
			if err = hermezDb.WriteBlockGlobalExitRoot(l2Block.L2BlockNumber, l2Block.GlobalExitRoot); err != nil {
				return fmt.Errorf("write block global exit root error: %w", err)
			}
			if err = hermezDb.WriteBlockL1BlockHash(l2Block.L2BlockNumber, l2Block.L1BlockHash); err != nil {
				return fmt.Errorf("write block global exit root error: %w", err)
			}
			if err = hermezDb.WriteReusedL1InfoTreeIndex(l2Block.L2BlockNumber); err != nil {
				return fmt.Errorf("write reused l1 info tree index error: %w", err)
			}
		}
	}

	if err := eriDb.WriteBody(bn, h.Hash(), txs); err != nil {
		return fmt.Errorf("write body error: %v", err)
	}

	if err := hermezDb.WriteForkId(l2Block.BatchNumber, uint64(l2Block.ForkId)); err != nil {
		return fmt.Errorf("write block batch error: %v", err)
	}

	if err := hermezDb.WriteBlockBatch(l2Block.L2BlockNumber, l2Block.BatchNumber); err != nil {
		return fmt.Errorf("write block batch error: %v", err)
	}

	return nil
}
