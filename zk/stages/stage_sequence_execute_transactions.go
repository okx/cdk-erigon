package stages

import (
	"context"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"

	"bytes"
	"io"

	mapset "github.com/deckarep/golang-set/v2"
	types2 "github.com/gateway-fm/cdk-erigon-lib/types"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/log/v3"
)

func getNextPoolTransactions(ctx context.Context, cfg SequenceBlockCfg, executionAt, forkId uint64, alreadyYielded mapset.Set[[32]byte], okPayPriority bool) ([]types.Transaction, error) {
	cfg.txPool.LockFlusher()
	defer cfg.txPool.UnlockFlusher()

	var transactions []types.Transaction
	var err error

	gasLimit := utils.GetBlockGasLimitForFork(forkId)

	if err := cfg.txPoolDb.View(ctx, func(poolTx kv.Tx) error {
		slots := types2.TxsRlp{}
		if _, _, err = cfg.txPool.YieldBest(cfg.yieldSize, &slots, poolTx, executionAt, gasLimit, alreadyYielded, okPayPriority); err != nil {
			return err
		}
		yieldedTxs, err := extractTransactionsFromSlot(&slots)
		if err != nil {
			return err
		}
		transactions = append(transactions, yieldedTxs...)
		return nil
	}); err != nil {
		return nil, err
	}

	return transactions, err
}

func getLimboTransaction(ctx context.Context, cfg SequenceBlockCfg, txHash *common.Hash) ([]types.Transaction, error) {
	cfg.txPool.LockFlusher()
	defer cfg.txPool.UnlockFlusher()

	var transactions []types.Transaction
	// ensure we don't spin forever looking for transactions, attempt for a while then exit up to the caller
	if err := cfg.txPoolDb.View(ctx, func(poolTx kv.Tx) error {
		slots, err := cfg.txPool.GetLimboTxRplsByHash(poolTx, txHash)
		if err != nil {
			return err
		}

		if slots != nil {
			transactions, err = extractTransactionsFromSlot(slots)
			if err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return transactions, nil
}

func extractTransactionsFromSlot(slot *types2.TxsRlp) ([]types.Transaction, error) {
	transactions := make([]types.Transaction, 0, len(slot.Txs))
	reader := bytes.NewReader([]byte{})
	stream := new(rlp.Stream)
	for idx, txBytes := range slot.Txs {
		reader.Reset(txBytes)
		stream.Reset(reader, uint64(len(txBytes)))
		transaction, err := types.DecodeTransaction(stream)
		if err == io.EOF {
			continue
		}
		if err != nil {
			return nil, err
		}
		var sender common.Address
		copy(sender[:], slot.Senders.At(idx))
		transaction.SetSender(sender)
		transactions = append(transactions, transaction)
	}
	return transactions, nil
}

func attemptAddTransaction(
	cfg SequenceBlockCfg,
	sdb *stageDb,
	ibs *state.IntraBlockState,
	batchCounters *vm.BatchCounterCollector,
	blockContext *evmtypes.BlockContext,
	header *types.Header,
	transaction types.Transaction,
	effectiveGasPrice uint8,
	l1Recovery bool,
	forkId, l1InfoIndex uint64,
	blockDataSizeChecker *BlockDataChecker,
	okPayPriority bool,
	okPayCounterLimitPercentage uint,
) (*types.Receipt, *core.ExecutionResult, bool, bool, error) {
	var batchDataOverflow, overflow, okPayOverflow bool
	var err error

	// For X Layer: check ok pay tx counter overflow
	sender, ok := transaction.GetSender()
	if !ok {
		signer := types.MakeSigner(cfg.chainConfig, header.Number.Uint64())
		sender, err = transaction.Sender(*signer)
		if err != nil {
			return nil, nil, false, false, err
		}
	}
	isOkPayTx := cfg.txPool.IsOkPayAddr(sender)

	txCounters := vm.NewTransactionCounter(transaction, sdb.smt.GetDepth(), uint16(forkId), cfg.zk.VirtualCountersSmtReduction, cfg.zk.ShouldCountersBeUnlimited(l1Recovery), isOkPayTx)
	overflow, err = batchCounters.AddNewTransactionCounters(txCounters)

	// run this only once the first time, do not add it on rerun
	if blockDataSizeChecker != nil {
		txL2Data, err := txCounters.GetL2DataCache()
		if err != nil {
			return nil, nil, false, false, err
		}
		batchDataOverflow = blockDataSizeChecker.AddTransactionData(txL2Data)
		if batchDataOverflow {
			log.Info("BatchL2Data limit reached. Not adding last transaction", "txHash", transaction.Hash())
		}
	}
	if err != nil {
		return nil, nil, false, false, err
	}
	anyOverflow := overflow || batchDataOverflow
	if anyOverflow && !l1Recovery {
		return nil, nil, true, false, nil
	}

	gasPool := new(core.GasPool).AddGas(transactionGasLimit)

	// set the counter collector on the config so that we can gather info during the execution
	cfg.zkVmConfig.CounterCollector = txCounters.ExecutionCounters()

	// TODO: possibly inject zero tracer here!

	snapshot := ibs.Snapshot()
	ibs.Prepare(transaction.Hash(), common.Hash{}, 0)
	evm := vm.NewZkEVM(*blockContext, evmtypes.TxContext{}, ibs, cfg.chainConfig, *cfg.zkVmConfig)

	gasUsed := header.GasUsed

	receipt, execResult, _, err := core.ApplyTransaction_zkevm(
		cfg.chainConfig,
		cfg.engine,
		evm,
		gasPool,
		ibs,
		noop,
		header,
		transaction,
		&gasUsed,
		effectiveGasPrice,
		false,
	)

	if err != nil {
		return nil, nil, false, false, err
	}

	if err = txCounters.ProcessTx(ibs, execResult.ReturnData); err != nil {
		return nil, nil, false, false, err
	}

	batchCounters.UpdateExecutionAndProcessingCountersCache(txCounters)
	// now that we have executed we can check again for an overflow
	if overflow, err = batchCounters.CheckForOverflow(l1InfoIndex != 0); err != nil {
		return nil, nil, false, false, err
	}

	if overflow {
		ibs.RevertToSnapshot(snapshot)
		return nil, nil, true, false, nil
	}

	// For X Layer: check ok pay tx counter overflow
	if isOkPayTx && okPayPriority {
		// now that we have executed we can check again for an overflow
		if okPayOverflow, err = batchCounters.CheckOkPayForOverflow(okPayCounterLimitPercentage); err != nil {
			return nil, nil, false, false, err
		}
		if okPayOverflow {
			ibs.RevertToSnapshot(snapshot)
			return nil, nil, false, true, nil
		}
	}

	// add the gas only if not reverted. This should not be moved above the overflow check
	header.GasUsed = gasUsed

	// we need to keep hold of the effective percentage used
	// todo [zkevm] for now we're hard coding to the max value but we need to calc this properly
	if err = sdb.hermezDb.WriteEffectiveGasPricePercentage(transaction.Hash(), effectiveGasPrice); err != nil {
		return nil, nil, false, false, err
	}

	ibs.FinalizeTx(evm.ChainRules(), noop)

	return receipt, execResult, false, false, nil
}
