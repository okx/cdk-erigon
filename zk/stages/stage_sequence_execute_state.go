package stages

import (
	"context"
	"fmt"
	"math"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/zk/l1_data"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"github.com/ledgerwatch/erigon/zk/txpool"
)

type BatchContext struct {
	ctx        context.Context
	cfg        *SequenceBlockCfg
	historyCfg *stagedsync.HistoryCfg
	s          *stagedsync.StageState
	sdb        *stageDb
}

func newBatchContext(ctx context.Context, cfg *SequenceBlockCfg, historyCfg *stagedsync.HistoryCfg, s *stagedsync.StageState, sdb *stageDb) *BatchContext {
	return &BatchContext{
		ctx:        ctx,
		cfg:        cfg,
		historyCfg: historyCfg,
		s:          s,
		sdb:        sdb,
	}
}

// TYPE BATCH STATE
type BatchState struct {
	forkId                        uint64
	batchNumber                   uint64
	hasExecutorForThisBatch       bool
	hasAnyTransactionsInThisBatch bool
	builtBlocks                   []uint64
	yieldedTransactions           mapset.Set[[32]byte]
	blockState                    *BlockState
	batchL1RecoveryData           *BatchL1RecoveryData
	limboRecoveryData             *LimboRecoveryData
}

func newBatchState(forkId, batchNumber uint64, hasExecutorForThisBatch, l1Recovery bool, txPool *txpool.TxPool) *BatchState {
	batchState := &BatchState{
		forkId:                        forkId,
		batchNumber:                   batchNumber,
		hasExecutorForThisBatch:       hasExecutorForThisBatch,
		hasAnyTransactionsInThisBatch: false,
		builtBlocks:                   make([]uint64, 0, 128),
		yieldedTransactions:           mapset.NewSet[[32]byte](),
		blockState:                    newBlockState(),
		batchL1RecoveryData:           nil,
		limboRecoveryData:             nil,
	}

	if l1Recovery {
		batchState.batchL1RecoveryData = newBatchL1RecoveryData(batchState)
	}

	limboHeaderTimestamp, limboTxHash := txPool.GetLimboTxHash(batchState.batchNumber)
	if limboTxHash != nil {
		batchState.limboRecoveryData = newLimboRecoveryData(limboHeaderTimestamp, limboTxHash)
	}

	return batchState
}

func (bs *BatchState) isL1Recovery() bool {
	return bs.batchL1RecoveryData != nil
}

func (bs *BatchState) isLimboRecovery() bool {
	return bs.limboRecoveryData != nil
}

func (bs *BatchState) isAnyRecovery() bool {
	return bs.isL1Recovery() || bs.isLimboRecovery()
}

func (bs *BatchState) isThereAnyTransactionsToRecover() bool {
	if !bs.isL1Recovery() {
		return false
	}

	return bs.blockState.hasAnyTransactionForInclusion() || bs.batchL1RecoveryData.recoveredBatchData.IsWorkRemaining
}

func (bs *BatchState) loadBlockL1RecoveryData(decodedBlocksIndex uint64) bool {
	decodedBatchL2Data, found := bs.batchL1RecoveryData.getDecodedL1RecoveredBatchDataByIndex(decodedBlocksIndex)
	bs.blockState.setBlockL1RecoveryData(decodedBatchL2Data)
	return found
}

// if not limbo set the limboHeaderTimestamp to the "default" value for "prepareHeader" function
func (bs *BatchState) getBlockHeaderForcedTimestamp() uint64 {
	if bs.isLimboRecovery() {
		return bs.limboRecoveryData.limboHeaderTimestamp
	}

	return math.MaxUint64
}

func (bs *BatchState) getCoinbase(cfg *SequenceBlockCfg) common.Address {
	if bs.isL1Recovery() {
		return bs.batchL1RecoveryData.recoveredBatchData.Coinbase
	}

	return cfg.zk.AddressSequencer
}

func (bs *BatchState) onAddedTransaction(transaction types.Transaction, receipt *types.Receipt, execResult *core.ExecutionResult, effectiveGas uint8) {
	bs.blockState.builtBlockElements.onFinishAddingTransaction(transaction, receipt, execResult, effectiveGas)
	bs.hasAnyTransactionsInThisBatch = true
}

func (bs *BatchState) onBuiltBlock(blockNumber uint64) {
	bs.builtBlocks = append(bs.builtBlocks, blockNumber)
}

// TYPE BATCH L1 RECOVERY DATA
type BatchL1RecoveryData struct {
	recoveredBatchDataSize int
	recoveredBatchData     *l1_data.DecodedL1Data
	batchState             *BatchState
}

func newBatchL1RecoveryData(batchState *BatchState) *BatchL1RecoveryData {
	return &BatchL1RecoveryData{
		batchState: batchState,
	}
}

func (batchL1RecoveryData *BatchL1RecoveryData) loadBatchData(sdb *stageDb) (err error) {
	batchL1RecoveryData.recoveredBatchData, err = l1_data.BreakDownL1DataByBatch(batchL1RecoveryData.batchState.batchNumber, batchL1RecoveryData.batchState.forkId, sdb.hermezDb.HermezDbReader)
	if err != nil {
		return err
	}

	batchL1RecoveryData.recoveredBatchDataSize = len(batchL1RecoveryData.recoveredBatchData.DecodedData)
	return nil
}

func (batchL1RecoveryData *BatchL1RecoveryData) hasAnyDecodedBlocks() bool {
	return batchL1RecoveryData.recoveredBatchDataSize > 0
}

func (batchL1RecoveryData *BatchL1RecoveryData) getInfoTreeIndex(sdb *stageDb) (uint64, error) {
	var infoTreeIndex uint64

	if batchL1RecoveryData.recoveredBatchData.L1InfoRoot == SpecialZeroIndexHash {
		return uint64(0), nil
	}

	infoTreeIndex, found, err := sdb.hermezDb.GetL1InfoTreeIndexByRoot(batchL1RecoveryData.recoveredBatchData.L1InfoRoot)
	if err != nil {
		return uint64(0), err
	}
	if !found {
		return uint64(0), fmt.Errorf("could not find L1 info tree index for root %s", batchL1RecoveryData.recoveredBatchData.L1InfoRoot.String())
	}

	return infoTreeIndex, nil
}

func (batchL1RecoveryData *BatchL1RecoveryData) getDecodedL1RecoveredBatchDataByIndex(decodedBlocksIndex uint64) (*zktx.DecodedBatchL2Data, bool) {
	if decodedBlocksIndex == uint64(batchL1RecoveryData.recoveredBatchDataSize) {
		return nil, false
	}

	return &batchL1RecoveryData.recoveredBatchData.DecodedData[decodedBlocksIndex], true
}

// TYPE LIMBO RECOVERY DATA
type LimboRecoveryData struct {
	limboHeaderTimestamp uint64
	limboTxHash          *common.Hash
}

func newLimboRecoveryData(limboHeaderTimestamp uint64, limboTxHash *common.Hash) *LimboRecoveryData {
	return &LimboRecoveryData{
		limboHeaderTimestamp: limboHeaderTimestamp,
		limboTxHash:          limboTxHash,
	}
}

// TYPE BLOCK STATE
type BlockState struct {
	transactionsForInclusion []types.Transaction
	builtBlockElements       BuiltBlockElements
	blockL1RecoveryData      *zktx.DecodedBatchL2Data
}

func newBlockState() *BlockState {
	return &BlockState{}
}

func (bs *BlockState) hasAnyTransactionForInclusion() bool {
	return len(bs.transactionsForInclusion) > 0
}

func (bs *BlockState) setBlockL1RecoveryData(blockL1RecoveryData *zktx.DecodedBatchL2Data) {
	bs.blockL1RecoveryData = blockL1RecoveryData

	if bs.blockL1RecoveryData != nil {
		bs.transactionsForInclusion = bs.blockL1RecoveryData.Transactions
	} else {
		bs.transactionsForInclusion = []types.Transaction{}
	}
}

func (bs *BlockState) getDeltaTimestamp() uint64 {
	if bs.blockL1RecoveryData != nil {
		return uint64(bs.blockL1RecoveryData.DeltaTimestamp)
	}

	return math.MaxUint64
}

func (bs *BlockState) getL1EffectiveGases(cfg SequenceBlockCfg, i int) uint8 {
	if bs.blockL1RecoveryData != nil {
		return bs.blockL1RecoveryData.EffectiveGasPricePercentages[i]
	}

	return DeriveEffectiveGasPrice(cfg, bs.transactionsForInclusion[i])
}

// TYPE BLOCK ELEMENTS
type BuiltBlockElements struct {
	transactions     []types.Transaction
	receipts         types.Receipts
	effectiveGases   []uint8
	executionResults []*core.ExecutionResult
}

func (bbe *BuiltBlockElements) resetBlockBuildingArrays() {
	bbe.transactions = []types.Transaction{}
	bbe.receipts = types.Receipts{}
	bbe.effectiveGases = []uint8{}
	bbe.executionResults = []*core.ExecutionResult{}
}

func (bbe *BuiltBlockElements) onFinishAddingTransaction(transaction types.Transaction, receipt *types.Receipt, execResult *core.ExecutionResult, effectiveGas uint8) {
	bbe.transactions = append(bbe.transactions, transaction)
	bbe.receipts = append(bbe.receipts, receipt)
	bbe.executionResults = append(bbe.executionResults, execResult)
	bbe.effectiveGases = append(bbe.effectiveGases, effectiveGas)
}
