package stages

import (
	"context"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/datadir"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	libstate "github.com/gateway-fm/cdk-erigon-lib/state"

	"math/big"

	"errors"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
	smtNs "github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"github.com/ledgerwatch/erigon/zk/txpool"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/log/v3"
)

const (
	logInterval = 20 * time.Second

	// stateStreamLimit - don't accumulate state changes if jump is bigger than this amount of blocks
	stateStreamLimit uint64 = 1_000

	transactionGasLimit = 30000000

	yieldSize = 1000 // arbitrary number defining how many transactions to yield from the pool at once
)

var (
	noop            = state.NewNoopWriter()
	blockDifficulty = new(big.Int).SetUint64(0)
)

type HasChangeSetWriter interface {
	ChangeSetWriter() *state.ChangeSetWriter
}

type ChangeSetHook func(blockNum uint64, wr *state.ChangeSetWriter)

type SequenceBlockCfg struct {
	db            kv.RwDB
	batchSize     datasize.ByteSize
	prune         prune.Mode
	changeSetHook ChangeSetHook
	chainConfig   *chain.Config
	engine        consensus.Engine
	zkVmConfig    *vm.ZkConfig
	badBlockHalt  bool
	stateStream   bool
	accumulator   *shards.Accumulator
	blockReader   services.FullBlockReader

	dirs      datadir.Dirs
	historyV3 bool
	syncCfg   ethconfig.Sync
	genesis   *types.Genesis
	agg       *libstate.AggregatorV3
	stream    *datastreamer.StreamServer
	zk        *ethconfig.Zk

	txPool   *txpool.TxPool
	txPoolDb kv.RwDB
}

func StageSequenceBlocksCfg(
	db kv.RwDB,
	pm prune.Mode,
	batchSize datasize.ByteSize,
	changeSetHook ChangeSetHook,
	chainConfig *chain.Config,
	engine consensus.Engine,
	vmConfig *vm.ZkConfig,
	accumulator *shards.Accumulator,
	stateStream bool,
	badBlockHalt bool,

	historyV3 bool,
	dirs datadir.Dirs,
	blockReader services.FullBlockReader,
	genesis *types.Genesis,
	syncCfg ethconfig.Sync,
	agg *libstate.AggregatorV3,
	stream *datastreamer.StreamServer,
	zk *ethconfig.Zk,

	txPool *txpool.TxPool,
	txPoolDb kv.RwDB,
) SequenceBlockCfg {
	return SequenceBlockCfg{
		db:            db,
		prune:         pm,
		batchSize:     batchSize,
		changeSetHook: changeSetHook,
		chainConfig:   chainConfig,
		engine:        engine,
		zkVmConfig:    vmConfig,
		dirs:          dirs,
		accumulator:   accumulator,
		stateStream:   stateStream,
		badBlockHalt:  badBlockHalt,
		blockReader:   blockReader,
		genesis:       genesis,
		historyV3:     historyV3,
		syncCfg:       syncCfg,
		agg:           agg,
		stream:        stream,
		zk:            zk,
		txPool:        txPool,
		txPoolDb:      txPoolDb,
	}
}

type stageDb struct {
	tx          kv.RwTx
	hermezDb    *hermez_db.HermezDb
	eridb       *db2.EriDb
	stateReader *state.PlainStateReader
	smt         *smtNs.SMT
}

func newStageDb(tx kv.RwTx) *stageDb {
	sdb := &stageDb{
		tx:          tx,
		hermezDb:    hermez_db.NewHermezDb(tx),
		eridb:       db2.NewEriDb(tx),
		stateReader: state.NewPlainStateReader(tx),
		smt:         nil,
	}
	sdb.smt = smtNs.NewSMT(sdb.eridb)

	return sdb
}

func prepareForkId(cfg SequenceBlockCfg, lastBatch, executionAt uint64, hermezDb *hermez_db.HermezDb) (uint64, error) {
	var forkId uint64 = 0
	var err error

	if executionAt == 0 {
		// capture the initial sequencer fork id for the first batch
		forkId = cfg.zk.SequencerInitialForkId
		if err := hermezDb.WriteForkId(1, forkId); err != nil {
			return forkId, err
		}
		if err := hermezDb.WriteForkIdBlockOnce(uint64(forkId), 1); err != nil {
			return forkId, err
		}
	} else {
		forkId, err = hermezDb.GetForkId(lastBatch)
		if err != nil {
			return forkId, err
		}
		if forkId == 0 {
			return forkId, errors.New("the network cannot have a 0 fork id")
		}
	}

	return forkId, nil
}

func prepareHeader(tx kv.RwTx, previousBlockNumber, deltaTimestamp, forkId uint64, coinbase common.Address) (*types.Header, *types.Block, error) {
	parentBlock, err := rawdb.ReadBlockByNumber(tx, previousBlockNumber)
	if err != nil {
		return nil, nil, err
	}

	// in the case of normal execution when not in l1 recovery
	// we want to generate the timestamp based on the current time.  When in recovery
	// we will pass a real delta which we then need to apply to the previous block timestamp
	useTimestampOffsetFromParentBlock := deltaTimestamp != math.MaxUint64

	nextBlockNum := previousBlockNumber + 1
	newBlockTimestamp := uint64(time.Now().Unix())
	if useTimestampOffsetFromParentBlock {
		newBlockTimestamp = parentBlock.Time() + deltaTimestamp
	}

	return &types.Header{
		ParentHash: parentBlock.Hash(),
		Coinbase:   coinbase,
		Difficulty: blockDifficulty,
		Number:     new(big.Int).SetUint64(nextBlockNum),
		GasLimit:   getGasLimit(forkId),
		Time:       newBlockTimestamp,
	}, parentBlock, nil
}

func prepareL1AndInfoTreeRelatedStuff(sdb *stageDb, decodedBlock *zktx.DecodedBatchL2Data, l1Recovery bool, proposedTimestamp uint64) (uint64, *zktypes.L1InfoTreeUpdate, uint64, common.Hash, common.Hash, bool, error) {
	var l1TreeUpdateIndex uint64
	var l1TreeUpdate *zktypes.L1InfoTreeUpdate
	var err error

	// if we are in a recovery state and recognise that a l1 info tree index has been reused
	// then we need to not include the GER and L1 block hash into the block info root calculation, so
	// we keep track of this here
	shouldWriteGerToContract := true

	l1BlockHash := common.Hash{}
	ger := common.Hash{}

	infoTreeIndexProgress, err := stages.GetStageProgress(sdb.tx, stages.HighestUsedL1InfoIndex)
	if err != nil {
		return infoTreeIndexProgress, l1TreeUpdate, l1TreeUpdateIndex, l1BlockHash, ger, shouldWriteGerToContract, err
	}

	if l1Recovery {
		l1TreeUpdateIndex = uint64(decodedBlock.L1InfoTreeIndex)
		l1TreeUpdate, err = sdb.hermezDb.GetL1InfoTreeUpdate(l1TreeUpdateIndex)
		if err != nil {
			return infoTreeIndexProgress, l1TreeUpdate, l1TreeUpdateIndex, l1BlockHash, ger, shouldWriteGerToContract, err
		}
		if infoTreeIndexProgress >= l1TreeUpdateIndex {
			shouldWriteGerToContract = false
		}
	} else {
		l1TreeUpdateIndex, l1TreeUpdate, err = calculateNextL1TreeUpdateToUse(infoTreeIndexProgress, sdb.hermezDb, proposedTimestamp)
		if err != nil {
			return infoTreeIndexProgress, l1TreeUpdate, l1TreeUpdateIndex, l1BlockHash, ger, shouldWriteGerToContract, err
		}
		if l1TreeUpdateIndex > 0 {
			infoTreeIndexProgress = l1TreeUpdateIndex
		}
	}

	// we only want GER and l1 block hash for indexes above 0 - 0 is a special case
	if l1TreeUpdate != nil && l1TreeUpdateIndex > 0 {
		l1BlockHash = l1TreeUpdate.ParentHash
		ger = l1TreeUpdate.GER
	}

	return infoTreeIndexProgress, l1TreeUpdate, l1TreeUpdateIndex, l1BlockHash, ger, shouldWriteGerToContract, nil
}

// will be called at the start of every new block created within a batch to figure out if there is a new GER
// we can use or not.  In the special case that this is the first block we just return 0 as we need to use the
// 0 index first before we can use 1+
func calculateNextL1TreeUpdateToUse(lastInfoIndex uint64, hermezDb *hermez_db.HermezDb, proposedTimestamp uint64) (uint64, *zktypes.L1InfoTreeUpdate, error) {
	// always default to 0 and only update this if the next available index has reached finality
	var nextL1Index uint64 = 0

	// check if the next index is there and if it has reached finality or not
	l1Info, err := hermezDb.GetL1InfoTreeUpdate(lastInfoIndex + 1)
	if err != nil {
		return 0, nil, err
	}

	// ensure that we are above the min timestamp for this index to use it
	if l1Info != nil && l1Info.Timestamp <= proposedTimestamp {
		nextL1Index = l1Info.Index
	}

	return nextL1Index, l1Info, nil
}

func updateSequencerProgress(tx kv.RwTx, newHeight uint64, newBatch uint64, l1InfoIndex uint64) error {
	// now update stages that will be used later on in stageloop.go and other stages. As we're the sequencer
	// we won't have headers stage for example as we're already writing them here
	if err := stages.SaveStageProgress(tx, stages.Execution, newHeight); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.Headers, newHeight); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.HighestSeenBatchNumber, newBatch); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.HighestUsedL1InfoIndex, l1InfoIndex); err != nil {
		return err
	}

	return nil
}

func doFinishBlockAndUpdateState(
	ctx context.Context,
	cfg SequenceBlockCfg,
	s *stagedsync.StageState,
	sdb *stageDb,
	ibs *state.IntraBlockState,
	header *types.Header,
	parentBlock *types.Block,
	forkId uint64,
	thisBatch uint64,
	ger common.Hash,
	l1BlockHash common.Hash,
	transactions []types.Transaction,
	receipts types.Receipts,
	effectiveGases []uint8,
	l1InfoIndex uint64,
) error {
	thisBlockNumber := header.Number.Uint64()

	if err := finaliseBlock(ctx, cfg, s, sdb, ibs, header, parentBlock, forkId, thisBatch, ger, l1BlockHash, transactions, receipts, effectiveGases); err != nil {
		return err
	}

	if err := updateSequencerProgress(sdb.tx, thisBlockNumber, thisBatch, l1InfoIndex); err != nil {
		return err
	}

	if cfg.accumulator != nil {
		txs, err := rawdb.RawTransactionsRange(sdb.tx, thisBlockNumber, thisBlockNumber)
		if err != nil {
			return err
		}
		cfg.accumulator.StartChange(thisBlockNumber, header.Hash(), txs, false)
	}

	return nil
}

type batchChecker interface {
	GetL1InfoTreeUpdate(idx uint64) (*zktypes.L1InfoTreeUpdate, error)
}

func checkForBadBatch(
	batchNo uint64,
	hermezDb batchChecker,
	latestTimestamp uint64,
	highestAllowedInfoTreeIndex uint64,
	limitTimestamp uint64,
	decodedBlocks []zktx.DecodedBatchL2Data,
) (bool, error) {
	timestamp := latestTimestamp

	for _, decodedBlock := range decodedBlocks {
		timestamp += uint64(decodedBlock.DeltaTimestamp)

		// now check the limit timestamp we can't have used l1 info tree index from the future
		if timestamp > limitTimestamp {
			log.Error("batch went above the limit timestamp", "batch", batchNo, "timestamp", timestamp, "limit_timestamp", limitTimestamp)
			return true, nil
		}

		if decodedBlock.L1InfoTreeIndex > 0 {
			// first check if we have knowledge of this index or not
			l1Info, err := hermezDb.GetL1InfoTreeUpdate(uint64(decodedBlock.L1InfoTreeIndex))
			if err != nil {
				return false, err
			}
			if l1Info == nil {
				// can't use an index that doesn't exist, so we have a bad batch
				log.Error("batch used info tree index that doesn't exist", "batch", batchNo, "index", decodedBlock.L1InfoTreeIndex)
				return true, nil
			}

			// we have an invalid batch if the block timestamp is lower than the l1 info min timestamp value
			if timestamp < l1Info.Timestamp {
				log.Error("batch used info tree index with timestamp lower than allowed", "batch", batchNo, "index", decodedBlock.L1InfoTreeIndex, "timestamp", timestamp, "min_timestamp", l1Info.Timestamp)
				return true, nil
			}

			// now finally check that the index used is lower or equal to the highest allowed index
			if uint64(decodedBlock.L1InfoTreeIndex) > highestAllowedInfoTreeIndex {
				log.Error("batch used info tree index higher than the current info tree root allows", "batch", batchNo, "index", decodedBlock.L1InfoTreeIndex, "highest_allowed", highestAllowedInfoTreeIndex)
				return true, nil
			}
		}
	}

	return false, nil
}
