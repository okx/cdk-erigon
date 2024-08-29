package stages

import (
	"context"
	"fmt"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk"
	"github.com/ledgerwatch/erigon/zk/utils"
)

func SpawnSequencingStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	ctx context.Context,
	cfg SequenceBlockCfg,
	historyCfg stagedsync.HistoryCfg,
	quiet bool,
) (err error) {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting sequencing stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished sequencing stage", logPrefix))

	sdb, err := newStageDb(ctx, cfg.db)
	if err != nil {
		return err
	}
	defer sdb.tx.Rollback()

	executionAt, err := s.ExecutionAt(sdb.tx)
	if err != nil {
		return err
	}

	lastBatch, err := stages.GetStageProgress(sdb.tx, stages.HighestSeenBatchNumber)
	if err != nil {
		return err
	}

	isLastBatchPariallyProcessed, err := sdb.hermezDb.GetIsBatchPartiallyProcessed(lastBatch)
	if err != nil {
		return err
	}

	forkId, err := prepareForkId(lastBatch, executionAt, sdb.hermezDb)
	if err != nil {
		return err
	}

	// stage loop should continue until we get the forkid from the L1 in a finalised block
	if forkId == 0 {
		log.Warn(fmt.Sprintf("[%s] ForkId is 0. Waiting for L1 to finalise a block...", logPrefix))
		time.Sleep(10 * time.Second)
		return nil
	}

	var block *types.Block
	runLoopBlocks := true
	batchContext := newBatchContext(ctx, &cfg, &historyCfg, s, sdb)
	batchState := newBatchState(forkId, prepareBatchNumber(lastBatch, isLastBatchPariallyProcessed), !isLastBatchPariallyProcessed && cfg.zk.HasExecutors(), cfg.zk.L1SyncStartBlock > 0, cfg.txPool)
	blockDataSizeChecker := newBlockDataChecker()
	streamWriter := newSequencerBatchStreamWriter(batchContext, batchState, lastBatch) // using lastBatch (rather than batchState.batchNumber) is not mistake

	// injected batch
	if executionAt == 0 {
		if err = processInjectedInitialBatch(batchContext, batchState); err != nil {
			return err
		}

		if err = cfg.datastreamServer.WriteWholeBatchToStream(logPrefix, sdb.tx, sdb.hermezDb.HermezDbReader, lastBatch, injectedBatchBatchNumber); err != nil {
			return err
		}

		return sdb.tx.Commit()
	}

	tryHaltSequencer(batchContext, batchState.batchNumber)

	if err := utils.UpdateZkEVMBlockCfg(cfg.chainConfig, sdb.hermezDb, logPrefix); err != nil {
		return err
	}

	batchCounters, err := prepareBatchCounters(batchContext, batchState, isLastBatchPariallyProcessed)
	if err != nil {
		return err
	}

	if !isLastBatchPariallyProcessed {
		// handle case where batch wasn't closed properly
		// close it before starting a new one
		// this occurs when sequencer was switched from syncer or sequencer datastream files were deleted
		// and datastream was regenerated
		if err = finalizeLastBatchInDatastreamIfNotFinalized(batchContext, batchState, executionAt); err != nil {
			return err
		}
	}

	if batchState.isL1Recovery() {
		if cfg.zk.L1SyncStopBatch > 0 && batchState.batchNumber > cfg.zk.L1SyncStopBatch {
			log.Info(fmt.Sprintf("[%s] L1 recovery has completed!", logPrefix), "batch", batchState.batchNumber)
			time.Sleep(1 * time.Second)
			return nil
		}

		log.Info(fmt.Sprintf("[%s] L1 recovery beginning for batch", logPrefix), "batch", batchState.batchNumber)

		// let's check if we have any L1 data to recover
		if err = batchState.batchL1RecoveryData.loadBatchData(sdb); err != nil {
			return err
		}

		if !batchState.batchL1RecoveryData.hasAnyDecodedBlocks() {
			log.Info(fmt.Sprintf("[%s] L1 recovery has completed!", logPrefix), "batch", batchState.batchNumber)
			time.Sleep(1 * time.Second)
			return nil
		}

		if handled, err := doCheckForBadBatch(batchContext, batchState, executionAt); err != nil || handled {
			return err
		}
	}

	batchTicker, logTicker, blockTicker := prepareTickers(batchContext.cfg)
	defer batchTicker.Stop()
	defer logTicker.Stop()
	defer blockTicker.Stop()

	log.Info(fmt.Sprintf("[%s] Starting batch %d...", logPrefix, batchState.batchNumber))

	for blockNumber := executionAt + 1; runLoopBlocks; blockNumber++ {
		log.Info(fmt.Sprintf("[%s] Starting block %d (forkid %v)...", logPrefix, blockNumber, batchState.forkId))
		logTicker.Reset(10 * time.Second)
		blockTicker.Reset(cfg.zk.SequencerBlockSealTime)

		if batchState.isL1Recovery() {
			didLoadedAnyDataForRecovery := batchState.loadBlockL1RecoveryData(blockNumber - (executionAt + 1))
			if !didLoadedAnyDataForRecovery {
				break
			}
		}

		l1InfoIndex, err := sdb.hermezDb.GetBlockL1InfoTreeIndex(blockNumber - 1)
		if err != nil {
			return err
		}

		header, parentBlock, err := prepareHeader(sdb.tx, blockNumber-1, batchState.blockState.getDeltaTimestamp(), batchState.getBlockHeaderForcedTimestamp(), batchState.forkId, batchState.getCoinbase(&cfg))
		if err != nil {
			return err
		}

		if batchDataOverflow := blockDataSizeChecker.AddBlockStartData(); batchDataOverflow {
			log.Info(fmt.Sprintf("[%s] BatchL2Data limit reached. Stopping.", logPrefix), "blockNumber", blockNumber)
			break
		}

		// timer: evm + smt
		t := utils.StartTimer("stage_sequence_execute", "evm", "smt")

		overflowOnNewBlock, err := batchCounters.StartNewBlock(l1InfoIndex != 0)
		if err != nil {
			return err
		}
		if !batchState.isAnyRecovery() && overflowOnNewBlock {
			break
		}

		infoTreeIndexProgress, l1TreeUpdate, l1TreeUpdateIndex, l1BlockHash, ger, shouldWriteGerToContract, err := prepareL1AndInfoTreeRelatedStuff(sdb, batchState, header.Time)
		if err != nil {
			return err
		}

		ibs := state.New(sdb.stateReader)
		getHashFn := core.GetHashFn(header, func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(sdb.tx, hash, number) })
		// For X Layer
		coinbase := batchState.getCoinbase(&cfg)
		blockContext := core.NewEVMBlockContext(header, getHashFn, cfg.engine, &coinbase, parentBlock.ExcessDataGas())
		batchState.blockState.builtBlockElements.resetBlockBuildingArrays()

		parentRoot := parentBlock.Root()
		if err = handleStateForNewBlockStarting(batchContext, ibs, blockNumber, batchState.batchNumber, header.Time, &parentRoot, l1TreeUpdate, shouldWriteGerToContract); err != nil {
			return err
		}

		// start waiting for a new transaction to arrive
		if !batchState.isAnyRecovery() {
			log.Info(fmt.Sprintf("[%s] Waiting for txs from the pool...", logPrefix))
		}

	LOOP_TRANSACTIONS:
		for {
			select {
			case <-logTicker.C:
				if !batchState.isAnyRecovery() {
					log.Info(fmt.Sprintf("[%s] Waiting some more for txs from the pool...", logPrefix))
				}
			case <-blockTicker.C:
				if !batchState.isAnyRecovery() {
					break LOOP_TRANSACTIONS
				}
			case <-batchTicker.C:
				if !batchState.isAnyRecovery() {
					runLoopBlocks = false
					break LOOP_TRANSACTIONS
				}
			default:
				if batchState.isLimboRecovery() {
					batchState.blockState.transactionsForInclusion, err = getLimboTransaction(ctx, cfg, batchState.limboRecoveryData.limboTxHash)
					if err != nil {
						return err
					}
				} else if !batchState.isL1Recovery() {
					batchState.blockState.transactionsForInclusion, err = getNextPoolTransactions(ctx, cfg, executionAt, batchState.forkId, batchState.yieldedTransactions)
					if err != nil {
						return err
					}
				}

				if len(batchState.blockState.transactionsForInclusion) == 0 {
					time.Sleep(250 * time.Millisecond)
				} else {
					log.Trace(fmt.Sprintf("[%s] Yielded transactions from the pool", logPrefix), "txCount", len(batchState.blockState.transactionsForInclusion))
				}

				for i, transaction := range batchState.blockState.transactionsForInclusion {
					txHash := transaction.Hash()
					effectiveGas := batchState.blockState.getL1EffectiveGases(cfg, i)

					// The copying of this structure is intentional
					backupDataSizeChecker := *blockDataSizeChecker
					receipt, execResult, anyOverflow, err := attemptAddTransaction(cfg, sdb, ibs, batchCounters, &blockContext, header, transaction, effectiveGas, batchState.isL1Recovery(), batchState.forkId, l1InfoIndex, &backupDataSizeChecker)
					if err != nil {
						if batchState.isLimboRecovery() {
							panic("limbo transaction has already been executed once so they must not fail while re-executing")
						}

						// if we are in recovery just log the error as a warning.  If the data is on the L1 then we should consider it as confirmed.
						// The executor/prover would simply skip a TX with an invalid nonce for example so we don't need to worry about that here.
						if batchState.isL1Recovery() {
							log.Warn(fmt.Sprintf("[%s] error adding transaction to batch during recovery: %v", logPrefix, err),
								"hash", txHash,
								"to", transaction.GetTo(),
							)
							continue
						}

						// if running in normal operation mode and error != nil then just allow the code to continue
						// It is safe because this approach ensures that the problematic transaction (the one that caused err != nil to be returned) is kept in yielded
						// Each transaction in yielded will be reevaluated at the end of each batch
					}

					if anyOverflow {
						if batchState.isLimboRecovery() {
							panic("limbo transaction has already been executed once so they must not overflow counters while re-executing")
						}

						if !batchState.isL1Recovery() {
							log.Info(fmt.Sprintf("[%s] overflowed adding transaction to batch", logPrefix), "batch", batchState.batchNumber, "tx-hash", txHash, "has-any-transactions-in-this-batch", batchState.hasAnyTransactionsInThisBatch)
							/*
								There are two cases when overflow could occur.
								1. The block DOES not contains any transactions.
									In this case it means that a single tx overflow entire zk-counters.
									In this case we mark it so. Once marked it will be discarded from the tx-pool async (once the tx-pool process the creation of a new batch)
									NB: The tx SHOULD not be removed from yielded set, because if removed, it will be picked again on next block. That's why there is i++. It ensures that removing from yielded will start after the problematic tx
								2. The block contains transactions.
									In this case, we just have to remove the transaction that overflowed the zk-counters and all transactions after it, from the yielded set.
									This removal will ensure that these transaction could be added in the next block(s)
							*/
							if !batchState.hasAnyTransactionsInThisBatch {
								cfg.txPool.MarkForDiscardFromPendingBest(txHash)
								log.Trace(fmt.Sprintf("single transaction %s overflow counters", txHash))
							}

							runLoopBlocks = false
							break LOOP_TRANSACTIONS
						}

					}

					if err == nil {
						blockDataSizeChecker = &backupDataSizeChecker
						batchState.onAddedTransaction(transaction, receipt, execResult, effectiveGas)
					}
				}

				if batchState.isL1Recovery() {
					// just go into the normal loop waiting for new transactions to signal that the recovery
					// has finished as far as it can go
					if batchState.isThereAnyTransactionsToRecover() {
						log.Info(fmt.Sprintf("[%s] L1 recovery no more transactions to recover", logPrefix))
					}

					break LOOP_TRANSACTIONS
				}

				if batchState.isLimboRecovery() {
					runLoopBlocks = false
					break LOOP_TRANSACTIONS
				}
			}
		}

		block, err = doFinishBlockAndUpdateState(batchContext, ibs, header, parentBlock, batchState, ger, l1BlockHash, l1TreeUpdateIndex, infoTreeIndexProgress, batchCounters)
		if err != nil {
			return err
		}

		if batchState.isLimboRecovery() {
			stateRoot := block.Root()
			cfg.txPool.UpdateLimboRootByTxHash(batchState.limboRecoveryData.limboTxHash, &stateRoot)
			return fmt.Errorf("[%s] %w: %s = %s", s.LogPrefix(), zk.ErrLimboState, batchState.limboRecoveryData.limboTxHash.Hex(), stateRoot.Hex())
		}

		t.LogTimer()
		gasPerSecond := float64(0)
		elapsedSeconds := t.Elapsed().Seconds()
		if elapsedSeconds != 0 {
			gasPerSecond = float64(block.GasUsed()) / elapsedSeconds
		}

		if gasPerSecond != 0 {
			log.Info(fmt.Sprintf("[%s] Finish block %d with %d transactions... (%d gas/s)", logPrefix, blockNumber, len(batchState.blockState.builtBlockElements.transactions), int(gasPerSecond)))
		} else {
			log.Info(fmt.Sprintf("[%s] Finish block %d with %d transactions...", logPrefix, blockNumber, len(batchState.blockState.builtBlockElements.transactions)))
		}

		// add a check to the verifier and also check for responses
		batchState.onBuiltBlock(blockNumber)

		if !batchState.isL1Recovery() {
			// commit block data here so it is accessible in other threads
			if errCommitAndStart := sdb.CommitAndStart(); errCommitAndStart != nil {
				return errCommitAndStart
			}
			defer sdb.tx.Rollback()
		}

		cfg.legacyVerifier.StartAsyncVerification(batchState.forkId, batchState.batchNumber, block.Root(), batchCounters.CombineCollectorsNoChanges().UsedAsMap(), batchState.builtBlocks, batchState.hasExecutorForThisBatch, batchContext.cfg.zk.SequencerBatchVerificationTimeout)

		// check for new responses from the verifier
		needsUnwind, err := updateStreamAndCheckRollback(batchContext, batchState, streamWriter, u)

		if !batchState.isL1Recovery() {
			// lets commit everything after updateStreamAndCheckRollback no matter of its result
			if errCommitAndStart := sdb.CommitAndStart(); errCommitAndStart != nil {
				return errCommitAndStart
			}
			defer sdb.tx.Rollback()
		}

		// check the return values of updateStreamAndCheckRollback
		if err != nil || needsUnwind {
			return err
		}
	}

	if batchState.isL1Recovery() {
		// lets commit everything after updateStreamAndCheckRollback no matter of its result
		if errCommitAndStart := sdb.CommitAndStart(); errCommitAndStart != nil {
			return errCommitAndStart
		}
		defer sdb.tx.Rollback()
	}

	cfg.legacyVerifier.Wait()
	needsUnwind, err := updateStreamAndCheckRollback(batchContext, batchState, streamWriter, u)
	if err != nil || needsUnwind {
		return err
	}

	if err = runBatchLastSteps(batchContext, batchState.batchNumber, block.NumberU64(), batchCounters); err != nil {
		return err
	}

	// For X Layer
	tryToSleepSequencer(cfg.zk.XLayer.SequencerBatchSleepDuration, logPrefix)

	return sdb.tx.Commit()
}
