// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package core implements the Ethereum consensus protocol.
package core

import (
	"fmt"
	"math/big"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"

	"github.com/ledgerwatch/erigon/chain"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/smt/pkg/blockinfo"
	txTypes "github.com/ledgerwatch/erigon/zk/tx"
)

// ExecuteBlockEphemerally runs a block from provided stateReader and
// writes the result to the provided stateWriter
func ExecuteBlockEphemerallyZk(
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
) (*EphemeralExecResult, error) {

	defer BlockExecutionTimer.UpdateDuration(time.Now())
	block.Uncles()
	ibs := state.New(stateReader)
	header := block.Header()
	blockTransactions := block.Transactions()
	blockGasLimit := block.GasLimit()

	//[hack] - on forkid7 this gas limit was used for execution but rpc is now returning forkid8 gas limit
	if !chainConfig.IsForkID8Elderberry(block.NumberU64()) {
		blockGasLimit = 18446744073709551615
	}

	gp := new(GasPool)
	gp.AddGas(blockGasLimit)

	var (
		rejectedTxs []*RejectedTx
		includedTxs types.Transactions
		receipts    types.Receipts
	)

	blockInfoTree, excessDataGas, err := PrepareBlockTxExecution(chainConfig, engine, chainReader, block, ibs, roHermezDb, blockGasLimit, vmConfig.ReadOnly)
	if err != nil {
		return nil, err
	}

	blockNum := block.NumberU64()
	noop := state.NewNoopWriter()
	logIndex := int64(0)
	usedGas := new(uint64)

	for txIndex, tx := range blockTransactions {
		ibs.Prepare(tx.Hash(), block.Hash(), txIndex)
		writeTrace := false
		if vmConfig.Debug && vmConfig.Tracer == nil {
			tracer, err := getTracer(txIndex, tx.Hash())
			if err != nil {
				return nil, fmt.Errorf("could not obtain tracer: %w", err)
			}
			vmConfig.Tracer = tracer
			writeTrace = true
		}

		gp.Reset(blockGasLimit)

		effectiveGasPricePercentage, err := roHermezDb.GetEffectiveGasPricePercentage(tx.Hash())
		if err != nil {
			return nil, err
		}

		zkConfig := vm.NewZkConfig(*vmConfig, nil)
		receipt, execResult, err := ApplyTransaction_zkevm(chainConfig, blockHashFunc, engine, nil, gp, ibs, noop, header, tx, usedGas, zkConfig, excessDataGas, effectiveGasPricePercentage)
		if err != nil {
			return nil, err
		}
		if writeTrace {
			if ftracer, ok := vmConfig.Tracer.(vm.FlushableTracer); ok {
				ftracer.Flush(tx)
			}

			vmConfig.Tracer = nil
		}

		//TODO: remove this after bug is fixed
		localReceipt := *receipt
		if execResult.Err == vm.ErrUnsupportedPrecompile {
			localReceipt.Status = 1
		}

		// receipt root holds the intermediate stateroot after the tx
		intermediateState, err := roHermezDb.GetIntermediateTxStateRoot(blockNum, tx.Hash())
		if err != nil {
			return nil, err
		}

		// forkid8 the poststate is empty
		// forkid8 also fixed the bugs with logs and cumulative gas used
		if !chainConfig.IsForkID8Elderberry(blockNum) {
			// the stateroot in the transactions that comes from the datastream
			// is the one after smart contract writes so it can't be used
			// but since pre forkid7 blocks have 1 tx only, we can use the block root
			if chainConfig.IsForkID7Etrog(blockNum) {
				receipt.PostState = intermediateState.Bytes()
			} else {
				receipt.PostState = header.Root.Bytes()
			}

			//[hack] log0 pre forkid8 are not included in the rpc logs
			// also pre forkid8 comulative gas used is same as gas used
			var fixedLogs types.Logs
			for _, l := range receipt.Logs {
				if len(l.Topics) == 0 && len(l.Data) == 0 {
					continue
				}
				fixedLogs = append(fixedLogs, l)
			}
			receipt.Logs = fixedLogs
			receipt.CumulativeGasUsed = receipt.GasUsed
		}

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
		}
		if !chainConfig.IsForkID7Etrog(block.NumberU64()) {
			if err := ibs.ScalableSetSmtRootHash(roHermezDb); err != nil {
				return nil, err
			}
		}

		if chainConfig.IsForkID7Etrog(blockNum) {
			txSender, _ := tx.GetSender()
			l2TxHash, err := txTypes.ComputeL2TxHash(
				tx.GetChainID().ToBig(),
				tx.GetValue(),
				tx.GetPrice(),
				tx.GetNonce(),
				tx.GetGas(),
				tx.GetTo(),
				&txSender,
				tx.GetData(),
			)
			if err != nil {
				return nil, err
			}

			//block info tree
			_, err = blockInfoTree.SetBlockTx(
				&l2TxHash,
				txIndex,
				&localReceipt,
				logIndex,
				*usedGas,
				effectiveGasPricePercentage,
			)
			if err != nil {
				return nil, err
			}
		}

		// increment logIndex for next turn
		// log idex counts all the logs in all txs in the block
		logIndex += int64(len(localReceipt.Logs))
	}

	var l2InfoRoot common.Hash
	if chainConfig.IsForkID7Etrog(blockNum) {
		// [zkevm] - set the block info tree root
		root, err := blockInfoTree.SetBlockGasUsed(*usedGas)
		if err != nil {
			return nil, err
		}
		l2InfoRoot = common.BigToHash(root)
	}

	ibs.PostExecuteStateSet(chainConfig, block.NumberU64(), &l2InfoRoot)

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
		if _, _, _, err := FinalizeBlockExecution(engine, stateReader, block.Header(), txs, block.Uncles(), stateWriter, chainConfig, ibs, receipts, block.Withdrawals(), chainReader, false, excessDataGas); err != nil {
			return nil, err
		}
	}
	blockLogs := ibs.Logs()
	execRs := &EphemeralExecResult{
		TxRoot:      types.DeriveSha(includedTxs),
		ReceiptRoot: receiptSha,
		Bloom:       bloom,
		LogsHash:    rlpHash(blockLogs),
		Receipts:    receipts,
		Difficulty:  (*math.HexOrDecimal256)(header.Difficulty),
		GasUsed:     math.HexOrDecimal64(*usedGas),
		Rejected:    rejectedTxs,
	}

	return execRs, nil
}

func PrepareBlockTxExecution(
	chainConfig *chain.Config,
	engine consensus.Engine,
	chainReader consensus.ChainHeaderReader,
	block *types.Block,
	ibs *state.IntraBlockState,
	roHermezDb state.ReadOnlyHermezDb,
	blockGasLimit uint64,
	readOnly bool,
) (blockInfoTree *blockinfo.BlockInfoTree, excessDataGas *big.Int, err error) {
	var blockNum uint64
	if block != nil {
		blockNum = block.NumberU64()
	}

	prevBlockheader := chainReader.GetHeaderByNumber(blockNum - 1)
	// TODO(eip-4844): understand why chainReader is sometimes nil (e.g. certain test cases)
	if prevBlockheader != nil {
		excessDataGas = prevBlockheader.ExcessDataGas
	}

	if !readOnly {
		if err := InitializeBlockExecution(engine, chainReader, block.Header(), block.Transactions(), block.Uncles(), chainConfig, ibs, excessDataGas); err != nil {
			return nil, nil, err
		}
	}

	if chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}

	///////////////////////////////////////////
	// [zkevm] set preexecution state 		 //
	///////////////////////////////////////////
	//[zkevm] - get the last batch number so we can check for empty batches in between it and the new one
	lastBatchInserted, err := roHermezDb.GetBatchNoByL2Block(blockNum - 1)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get last batch inserted: %v", err)
	}

	// write batches between last block and this if they exist
	currentBatch, err := roHermezDb.GetBatchNoByL2Block(blockNum)
	if err != nil {
		return nil, nil, err
	}

	//[zkevm] get batches between last block and this one
	// plus this blocks ger
	gersInBetween, err := roHermezDb.GetBatchGlobalExitRoots(lastBatchInserted, currentBatch)
	if err != nil {
		return nil, nil, err
	}

	blockGer, err := roHermezDb.GetBlockGlobalExitRoot(blockNum)
	if err != nil {
		return nil, nil, err
	}
	l1BlockHash, err := roHermezDb.GetBlockL1BlockHash(blockNum)
	if err != nil {
		return nil, nil, err
	}

	blockTime := block.Time()
	prevBlockRoot := prevBlockheader.Root
	l1InfoTreeIndexReused, err := roHermezDb.GetReusedL1InfoTreeIndex(blockNum)
	if err != nil {
		return nil, nil, err
	}
	ibs.SyncerPreExecuteStateSet(chainConfig, blockNum, blockTime, &prevBlockRoot, &blockGer, &l1BlockHash, gersInBetween, l1InfoTreeIndexReused)
	///////////////////////////////////////////
	// [zkevm] finish set preexecution state //
	///////////////////////////////////////////

	///////////////////////////////////////////
	// [zkevm] initiate block info tree		 //
	///////////////////////////////////////////
	blockInfoTree = blockinfo.NewBlockInfoTree()
	if chainConfig.IsForkID7Etrog(blockNum) {
		coinbase := block.Coinbase()

		// this is a case when we have injected batches
		// we have to save the l1block hash and in this case we have to add
		// the ger in that l1 bloc k into the block info tree
		// even though it is previously added to the state
		if l1BlockHash != (common.Hash{}) && blockGer == (common.Hash{}) {
			blockGer, err = roHermezDb.GetGerForL1BlockHash(l1BlockHash)
			if err != nil {
				return nil, nil, err
			}
		}

		if err := blockInfoTree.InitBlockHeader(
			&prevBlockRoot,
			&coinbase,
			blockNum,
			blockGasLimit,
			blockTime,
			&blockGer,
			&l1BlockHash,
		); err != nil {
			return nil, nil, err
		}
	}

	return blockInfoTree, excessDataGas, nil
}

func FinalizeBlockExecutionWithHistoryWrite(
	engine consensus.Engine, stateReader state.StateReader,
	header *types.Header, txs types.Transactions, uncles []*types.Header,
	stateWriter state.WriterWithChangeSets, cc *chain.Config,
	ibs *state.IntraBlockState, receipts types.Receipts,
	withdrawals []*types.Withdrawal, headerReader consensus.ChainHeaderReader,
	isMining bool, excessDataGas *big.Int,
) (newBlock *types.Block, newTxs types.Transactions, newReceipt types.Receipts, err error) {
	newBlock, newTxs, newReceipt, err = FinalizeBlockExecution(
		engine,
		stateReader,
		header,
		txs,
		uncles,
		stateWriter,
		cc,
		ibs,
		receipts,
		withdrawals,
		headerReader,
		isMining,
		excessDataGas,
	)

	if err := stateWriter.WriteHistory(); err != nil {
		return nil, nil, nil, fmt.Errorf("writing history for block %d failed: %w", header.Number.Uint64(), err)
	}

	return newBlock, newTxs, newReceipt, nil
}
