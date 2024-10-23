package core

import (
	"context"
	"fmt"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/go-redis/redis/v8"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/smt/pkg/blockinfo"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/erigon/zk/utils"
	"math/big"
	"time"
)

func FinalizeBlockExecutionDDSProducer(
	rdb *redis.Client,
	engine consensus.Engine, stateReader state.StateReader,
	header *types.Header, txs types.Transactions, uncles []*types.Header,
	stateWriter state.WriterWithChangeSets, cc *chain.Config,
	ibs *state.IntraBlockState, receipts types.Receipts,
	withdrawals []*types.Withdrawal, headerReader consensus.ChainHeaderReader,
	isMining bool, excessDataGas *big.Int,
) (newBlock *types.Block, newTxs types.Transactions, newReceipt types.Receipts, err error) {
	syscall := func(contract common.Address, data []byte) ([]byte, error) {
		return SysCallContract(contract, data, *cc, ibs, header, engine, false /* constCall */, excessDataGas)
	}
	if isMining {
		newBlock, newTxs, newReceipt, err = engine.FinalizeAndAssemble(cc, header, ibs, txs, uncles, receipts, withdrawals, headerReader, syscall, nil)
	} else {
		_, _, err = engine.Finalize(cc, header, ibs, txs, uncles, receipts, withdrawals, headerReader, syscall)
	}
	if err != nil {
		return nil, nil, nil, err
	}

	// producer
	deltaBytes, err := ibs.CommitBlockDDSProducer(cc.Rules(header.Number.Uint64(), header.Time), stateWriter)
	if err != nil {
		panic(err)
	}
	if err = rdb.Set(context.Background(), GenDDSKey("state", header.Number.Uint64()), deltaBytes, 0).Err(); err != nil {
		panic("Failed redis state_" + header.Number.String())
	}

	if err := stateWriter.WriteChangeSets(); err != nil {
		return nil, nil, nil, fmt.Errorf("writing changesets for block %d failed: %w", header.Number.Uint64(), err)
	}
	return newBlock, newTxs, newReceipt, nil
}

func ExecuteBlockEphemerallyZkDDSProducer(
	rdb *redis.Client,
	chainConfig *chain.Config,
	vmConfig *vm.Config,
	blockHashFunc func(n uint64) common.Hash,
	engine consensus.Engine,
	block *types.Block,
	stateReader state.StateReader,
	stateWriter state.WriterWithChangeSets,
	chainReader consensus.ChainHeaderReader,
	getTracer func(txIndex int, txHash common.Hash) (vm.EVMLogger, error),
	roHermezDb state.ReadOnlyHermezDb,
	prevBlockRoot *common.Hash,
) (*EphemeralExecResultZk, error) {

	defer BlockExecutionTimer.UpdateDuration(time.Now())
	block.Uncles()
	ibs := state.New(stateReader)
	header := block.Header()
	blockTransactions := block.Transactions()
	blockGasLimit := block.GasLimit()

	if !chainConfig.IsForkID8Elderberry(block.NumberU64()) {
		blockGasLimit = utils.ForkId7BlockGasLimit
	}

	gp := new(GasPool).AddGas(blockGasLimit)

	var (
		rejectedTxs []*RejectedTx
		includedTxs types.Transactions
		receipts    types.Receipts

		// For X Layer
		blockInnerTxs [][]*zktypes.InnerTx
	)

	blockContext, excessDataGas, ger, l1Blockhash, err := PrepareBlockTxExecution(chainConfig, vmConfig, blockHashFunc, nil, engine, chainReader, block, ibs, roHermezDb, blockGasLimit)
	if err != nil {
		return nil, err
	}

	blockNum := block.NumberU64()
	usedGas := new(uint64)
	txInfos := []blockinfo.ExecutedTxInfo{}

	for txIndex, tx := range blockTransactions {
		writeTrace := false
		if vmConfig.Debug && vmConfig.Tracer == nil {
			tracer, err := getTracer(txIndex, tx.Hash())
			if err != nil {
				return nil, fmt.Errorf("could not obtain tracer: %w", err)
			}
			vmConfig.Tracer = tracer
			writeTrace = true
		}
		txHash := tx.Hash()
		evm, effectiveGasPricePercentage, err := PrepareForTxExecution(chainConfig, vmConfig, blockContext, roHermezDb, ibs, block, &txHash, txIndex)
		if err != nil {
			return nil, err
		}

		receipt, execResult, innerTxs, err := ApplyTransaction_zkevm(chainConfig, engine, evm, gp, ibs, state.NewNoopWriter(), header, tx, usedGas, effectiveGasPricePercentage, true)
		if err != nil {
			return nil, err
		}
		if writeTrace {
			if ftracer, ok := vmConfig.Tracer.(vm.FlushableTracer); ok {
				ftracer.Flush(tx)
			}

			vmConfig.Tracer = nil
		}

		localReceipt := CreateReceiptForBlockInfoTree(receipt, chainConfig, blockNum, execResult)
		ProcessReceiptForBlockExecution(receipt, roHermezDb, chainConfig, blockNum, header, tx)

		if err != nil {
			if !vmConfig.StatelessExec {
				return nil, fmt.Errorf("could not apply tx %d from block %d [%v]: %w", txIndex, block.NumberU64(), tx.Hash().Hex(), err)
			}
			rejectedTxs = append(rejectedTxs, &RejectedTx{txIndex, err.Error()})
		} else {
			includedTxs = append(includedTxs, tx)
			if !vmConfig.NoReceipts {
				receipts = append(receipts, receipt)
			}
			// For X Layer
			if !vmConfig.NoInnerTxs {
				blockInnerTxs = append(blockInnerTxs, innerTxs)
			}
		}
		if !chainConfig.IsForkID7Etrog(block.NumberU64()) {
			if err := ibs.ScalableSetSmtRootHash(roHermezDb); err != nil {
				return nil, err
			}
		}

		txSender, ok := tx.GetSender()
		if !ok {
			signer := types.MakeSigner(chainConfig, blockNum)
			txSender, err = tx.Sender(*signer)
			if err != nil {
				return nil, err
			}
		}

		txInfos = append(txInfos, blockinfo.ExecutedTxInfo{
			Tx:                tx,
			Receipt:           localReceipt,
			EffectiveGasPrice: effectiveGasPricePercentage,
			Signer:            &txSender,
		})
	}

	var l2InfoRoot *common.Hash
	if chainConfig.IsForkID7Etrog(blockNum) {
		l2InfoRoot, err = blockinfo.BuildBlockInfoTree(
			&header.Coinbase,
			header.Number.Uint64(),
			header.Time,
			blockGasLimit,
			*usedGas,
			*ger,
			*l1Blockhash,
			*prevBlockRoot,
			&txInfos,
		)
		if err != nil {
			return nil, err
		}
	}

	ibs.PostExecuteStateSet(chainConfig, block.NumberU64(), l2InfoRoot)

	receiptSha := types.DeriveSha(receipts)
	// [zkevm] todo
	//if !vmConfig.StatelessExec && chainConfig.IsByzantium(header.Number.Uint64()) && !vmConfig.NoReceipts && receiptSha != block.ReceiptHash() {
	//	return nil, fmt.Errorf("mismatched receipt headers for block %d (%s != %s)", block.NumberU64(), receiptSha.Hex(), block.ReceiptHash().Hex())
	//}

	// in zkEVM we don't have headers to check GasUsed against
	//if !vmConfig.StatelessExec && *usedGas != header.GasUsed && header.GasUsed > 0 {
	//	return nil, fmt.Errorf("gas used by execution: %d, in header: %d", *usedGas, header.GasUsed)
	//}

	var bloom types.Bloom
	if !vmConfig.NoReceipts {
		bloom = types.CreateBloom(receipts)
		// [zkevm] todo
		//if !vmConfig.StatelessExec && bloom != header.Bloom {
		//	return nil, fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
		//}
	}
	if !vmConfig.ReadOnly {
		txs := blockTransactions
		if _, _, _, err := FinalizeBlockExecutionDDSProducer(rdb, engine, stateReader, block.Header(), txs, block.Uncles(), stateWriter, chainConfig, ibs, receipts, block.Withdrawals(), chainReader, false, excessDataGas); err != nil {
			return nil, err
		}
	}
	blockLogs := ibs.Logs()
	execRs := &EphemeralExecResultZk{
		EphemeralExecResult: &EphemeralExecResult{
			TxRoot:      types.DeriveSha(includedTxs),
			ReceiptRoot: receiptSha,
			Bloom:       bloom,
			LogsHash:    rlpHash(blockLogs),
			Receipts:    receipts,
			Difficulty:  (*math.HexOrDecimal256)(header.Difficulty),
			GasUsed:     math.HexOrDecimal64(*usedGas),
			Rejected:    rejectedTxs,
			// For X Layer
			InnerTxs: blockInnerTxs,
		},
		BlockInfoTree: l2InfoRoot,
	}

	return execRs, nil
}

func ExecuteBlockEphemerallyZkDDSConsumer(rdb *redis.Client, cc *chain.Config, block *types.Block, stateReader state.StateReader, stateWriter state.WriterWithChangeSets) {
	ibs := state.New(stateReader)
	header := block.Header()
	redisRs, err := rdb.Get(context.Background(), GenDDSKey("state", header.Number.Uint64())).Bytes()
	if err == nil && len(redisRs) > 0 {
		// consumer
		if err = ibs.CommitBlockDDSConsumer(cc.Rules(header.Number.Uint64(), header.Time), stateWriter, redisRs); err != nil {
			panic(err)
		}
	}
}

func GenDDSKey(key string, blockNum uint64) string {
	return fmt.Sprintf("%s_%d", key, blockNum)
}
