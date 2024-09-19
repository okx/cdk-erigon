package stages

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
)

func prepareBatchCounters(batchContext *BatchContext, batchState *BatchState, intermediateUsedCounters *vm.Counters) (*vm.BatchCounterCollector, error) {
	return vm.NewBatchCounterCollector(batchContext.sdb.smt.GetDepth(), uint16(batchState.forkId), batchContext.cfg.zk.VirtualCountersSmtReduction, batchContext.cfg.zk.ShouldCountersBeUnlimited(batchState.isL1Recovery()), intermediateUsedCounters), nil
}

func doCheckForBadBatch(batchContext *BatchContext, batchState *BatchState, thisBlock uint64) (bool, error) {
	infoTreeIndex, err := batchState.batchL1RecoveryData.getInfoTreeIndex(batchContext.sdb)
	if err != nil {
		return false, err
	}

	// now let's detect a bad batch and skip it if we have to
	currentBlock, err := rawdb.ReadBlockByNumber(batchContext.sdb.tx, thisBlock)
	if err != nil {
		return false, err
	}

	badBatch, err := checkForBadBatch(batchState.batchNumber, batchContext.sdb.hermezDb, currentBlock.Time(), infoTreeIndex, batchState.batchL1RecoveryData.recoveredBatchData.LimitTimestamp, batchState.batchL1RecoveryData.recoveredBatchData.DecodedData)
	if err != nil {
		return false, err
	}

	if !badBatch {
		return false, nil
	}

	log.Info(fmt.Sprintf("[%s] Skipping bad batch %d...", batchContext.s.LogPrefix(), batchState.batchNumber))
	// store the fact that this batch was invalid during recovery - will be used for the stream later
	if err = batchContext.sdb.hermezDb.WriteInvalidBatch(batchState.batchNumber); err != nil {
		return false, err
	}
	if err = batchContext.sdb.hermezDb.WriteBatchCounters(currentBlock.NumberU64(), map[string]int{}); err != nil {
		return false, err
	}
	if err = stages.SaveStageProgress(batchContext.sdb.tx, stages.HighestSeenBatchNumber, batchState.batchNumber); err != nil {
		return false, err
	}
	if err = batchContext.sdb.hermezDb.WriteForkId(batchState.batchNumber, batchState.forkId); err != nil {
		return false, err
	}
	if err = batchContext.sdb.tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func updateStreamAndCheckRollback(
	batchContext *BatchContext,
	batchState *BatchState,
	streamWriter *SequencerBatchStreamWriter,
	u stagedsync.Unwinder,
) (bool, error) {
	checkedVerifierBundles, err := streamWriter.CommitNewUpdates()
	if err != nil {
		return false, err
	}

	infiniteLoop := func(batchNumber uint64) {
		// this infinite loop will make the node to print the error once every minute therefore preventing it for creating new blocks
		for {
			log.Error(fmt.Sprintf("[%s] identified an invalid batch with number %d", batchContext.s.LogPrefix(), batchNumber))
			time.Sleep(time.Minute)
		}
	}

	for _, verifierBundle := range checkedVerifierBundles {
		if verifierBundle.Response.Valid {
			continue
		}

		// The sequencer can goes to this point of the code only in L1Recovery mode or Default Mode.
		// There is no way to get here in LimboRecoveryMode
		// If we are here in L1RecoveryMode then let's stop everything by using an infinite loop because something is quite wrong
		// If we are here in Default mode and limbo is disabled then again do the same as in L1RecoveryMode
		// If we are here in Default mode and limbo is enabled then continue normal flow
		if batchState.isL1Recovery() || !batchContext.cfg.zk.Limbo {
			infiniteLoop(verifierBundle.Request.BatchNumber)
		}

		if err = handleLimbo(batchContext, batchState, verifierBundle); err != nil {
			return false, err
		}

		unwindTo := verifierBundle.Request.GetLastBlockNumber() - 1

		// for unwind we supply the block number X-1 of the block we want to remove, but supply the hash of the block
		// causing the unwind.
		unwindHeader := rawdb.ReadHeaderByNumber(batchContext.sdb.tx, verifierBundle.Request.GetLastBlockNumber())
		if unwindHeader == nil {
			return false, fmt.Errorf("could not find header for block %d", verifierBundle.Request.GetLastBlockNumber())
		}

		log.Warn(fmt.Sprintf("[%s] Block is invalid - rolling back", batchContext.s.LogPrefix()), "badBlock", verifierBundle.Request.GetLastBlockNumber(), "unwindTo", unwindTo, "root", unwindHeader.Root)

		u.UnwindTo(unwindTo, unwindHeader.Hash())
		streamWriter.legacyVerifier.CancelAllRequests()
		return true, nil
	}

	return false, nil
}
