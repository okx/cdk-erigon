package jsonrpc

import (
	"context"
	"fmt"

	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/turbo/transactions"
)

var logConfig = &logger.LogConfig{
	DisableMemory:     false,
	DisableStack:      false,
	DisableStorage:    false,
	DisableReturnData: false,
	Debug:             true,
	Overrides:         nil,
}

func (api *APIImpl) getInternalTransactionsByTracer(ctx context.Context, txHash common.Hash, stream *jsoniter.Stream) error {
	var config *tracers.TraceConfig_ZkEvm
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	// Retrieve the transaction and assemble its EVM context
	var isBorStateSyncTxn bool
	blockNum, ok, err := api.txnLookup(ctx, tx, txHash)
	if err != nil {
		stream.WriteNil()
		return err
	}
	if !ok {
		if chainConfig.Bor == nil {
			stream.WriteNil()
			return nil
		}

		// otherwise this may be a bor state sync transaction - check
		blockNum, ok, err = api._blockReader.EventLookup(ctx, tx, txHash)
		if err != nil {
			stream.WriteNil()
			return err
		}
		if !ok {
			stream.WriteNil()
			return nil
		}
		if config == nil || config.BorTraceEnabled == nil || *config.BorTraceEnabled == false {
			stream.WriteEmptyArray() // matches maticnetwork/bor API behaviour for consistency
			return nil
		}

		isBorStateSyncTxn = true
	}

	// check pruning to ensure we have history at this block level
	err = api.BaseAPI.checkPruneHistory(tx, blockNum)
	if err != nil {
		stream.WriteNil()
		return err
	}

	block, err := api.blockByNumberWithSenders(ctx, tx, blockNum)
	if err != nil {
		stream.WriteNil()
		return err
	}
	if block == nil {
		stream.WriteNil()
		return nil
	}

	var txnIndex int
	var txn types.Transaction
	for i := 0; i < block.Transactions().Len() && !isBorStateSyncTxn; i++ {
		transaction := block.Transactions()[i]
		if transaction.Hash() == txHash {
			txnIndex = i
			txn = transaction
			break
		}
	}
	if txn == nil {
		if isBorStateSyncTxn {
			// bor state sync tx is appended at the end of the block
			txnIndex = block.Transactions().Len()
		} else {
			stream.WriteNil()
			return fmt.Errorf("transaction %#x not found", txHash)
		}
	}

	engine := api.engine()

	txEnv, err := transactions.ComputeTxEnv_ZkEvm(ctx, engine, block, chainConfig, api._blockReader, tx, int(txnIndex), api.historyV3(tx))
	if err != nil {
		stream.WriteNil()
		return err
	}

	tracer := "okTracer"
	okTracerConfig := &tracers.TraceConfig_ZkEvm{
		LogConfig: logConfig,
		Tracer:    &tracer,
	}

	return transactions.TraceTx(ctx, txEnv.Msg, txEnv.BlockContext, txEnv.TxContext, txEnv.Ibs, okTracerConfig, chainConfig, stream, api.evmCallTimeout)
}
