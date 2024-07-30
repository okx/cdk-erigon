package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/hexutility"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	jsoniter "github.com/json-iterator/go"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	types "github.com/ledgerwatch/erigon/zk/rpcdaemon"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	"github.com/ledgerwatch/erigon/zk/syncer"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/erigon/zk/witness"
	"github.com/ledgerwatch/erigon/zkevm/hex"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/client"
)

var sha3UncleHash = common.HexToHash("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347")

const ApiRollupId = 1 // todo [zkevm] this should be read from config really

// ZkEvmAPI is a collection of functions that are exposed in the
type ZkEvmAPI interface {
	ConsolidatedBlockNumber(ctx context.Context) (hexutil.Uint64, error)
	IsBlockConsolidated(ctx context.Context, blockNumber rpc.BlockNumber) (bool, error)
	IsBlockVirtualized(ctx context.Context, blockNumber rpc.BlockNumber) (bool, error)
	BatchNumberByBlockNumber(ctx context.Context, blockNumber rpc.BlockNumber) (hexutil.Uint64, error)
	BatchNumber(ctx context.Context) (hexutil.Uint64, error)
	VirtualBatchNumber(ctx context.Context) (hexutil.Uint64, error)
	VerifiedBatchNumber(ctx context.Context) (hexutil.Uint64, error)
	GetBatchByNumber(ctx context.Context, batchNumber rpc.BlockNumber, fullTx *bool) (json.RawMessage, error)
	GetFullBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (types.Block, error)
	GetFullBlockByHash(ctx context.Context, hash common.Hash, fullTx bool) (types.Block, error)
	// GetBroadcastURI(ctx context.Context) (string, error)
	GetWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, mode *WitnessMode, debug *bool) (hexutility.Bytes, error)
	GetBlockRangeWitness(ctx context.Context, startBlockNrOrHash rpc.BlockNumberOrHash, endBlockNrOrHash rpc.BlockNumberOrHash, mode *WitnessMode, debug *bool) (hexutility.Bytes, error)
	GetBatchWitness(ctx context.Context, batchNumber uint64, mode *WitnessMode) (interface{}, error)
	GetProverInput(ctx context.Context, batchNumber uint64, mode *WitnessMode, debug *bool) (*legacy_executor_verifier.RpcPayload, error)
	GetLatestGlobalExitRoot(ctx context.Context) (common.Hash, error)
	GetExitRootsByGER(ctx context.Context, globalExitRoot common.Hash) (*ZkExitRoots, error)
	GetL2BlockInfoTree(ctx context.Context, blockNum rpc.BlockNumberOrHash) (json.RawMessage, error)
	EstimateCounters(ctx context.Context, argsOrNil *zkevmRPCTransaction) (json.RawMessage, error)
	TraceTransactionCounters(ctx context.Context, hash common.Hash, config *tracers.TraceConfig_ZkEvm, stream *jsoniter.Stream) error
	GetBatchCountersByNumber(ctx context.Context, batchNumRpc rpc.BlockNumber) (res json.RawMessage, err error)
	GetExitRootTable(ctx context.Context) ([]l1InfoTreeData, error)
}

// APIImpl is implementation of the ZkEvmAPI interface based on remote Db access
type ZkEvmAPIImpl struct {
	ethApi *APIImpl

	db              kv.RoDB
	ReturnDataLimit int
	config          *ethconfig.Config
	l1Syncer        *syncer.L1Syncer
	l2SequencerUrl  string
}

// NewEthAPI returns ZkEvmAPIImpl instance
func NewZkEvmAPI(
	base *APIImpl,
	db kv.RoDB,
	returnDataLimit int,
	zkConfig *ethconfig.Config,
	l1Syncer *syncer.L1Syncer,
	l2SequencerUrl string,
) *ZkEvmAPIImpl {
	return &ZkEvmAPIImpl{
		ethApi:          base,
		db:              db,
		ReturnDataLimit: returnDataLimit,
		config:          zkConfig,
		l1Syncer:        l1Syncer,
		l2SequencerUrl:  l2SequencerUrl,
	}
}

// ConsolidatedBlockNumber returns the latest consolidated block number
// Once a batch is verified, it is connected to the blockchain, and the block number of the most recent block in that batch
// becomes the "consolidated block number.”
func (api *ZkEvmAPIImpl) ConsolidatedBlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return hexutil.Uint64(0), err
	}
	defer tx.Rollback()

	highestVerifiedBatchNo, err := stages.GetStageProgress(tx, stages.L1VerificationsBatchNo)
	if err != nil {
		return hexutil.Uint64(0), err
	}

	blockNum, err := getLastBlockInBatchNumber(tx, highestVerifiedBatchNo)
	if err != nil {
		return hexutil.Uint64(0), err
	}

	return hexutil.Uint64(blockNum), nil
}

// IsBlockConsolidated returns true if the block is consolidated
func (api *ZkEvmAPIImpl) IsBlockConsolidated(ctx context.Context, blockNumber rpc.BlockNumber) (bool, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	batchNum, err := getBatchNoByL2Block(tx, uint64(blockNumber.Int64()))
	if err != nil {
		return false, err
	}

	highestVerifiedBatchNo, err := stages.GetStageProgress(tx, stages.L1VerificationsBatchNo)
	if err != nil {
		return false, err
	}

	return batchNum <= highestVerifiedBatchNo, nil
}

// IsBlockVirtualized returns true if the block is virtualized (not confirmed on the L1 but exists in the L1 smart contract i.e. sequenced)
func (api *ZkEvmAPIImpl) IsBlockVirtualized(ctx context.Context, blockNumber rpc.BlockNumber) (bool, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	batchNum, err := getBatchNoByL2Block(tx, uint64(blockNumber.Int64()))
	if err != nil {
		return false, err
	}

	hermezDb := hermez_db.NewHermezDbReader(tx)
	latestSequencedBatch, err := hermezDb.GetLatestSequence()
	if err != nil {
		return false, err
	}

	// if the batch is lower than the latest sequenced then it must be virtualized
	return batchNum <= latestSequencedBatch.BatchNo, nil
}

// BatchNumberByBlockNumber returns the batch number of the block
func (api *ZkEvmAPIImpl) BatchNumberByBlockNumber(ctx context.Context, blockNumber rpc.BlockNumber) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return hexutil.Uint64(0), err
	}
	defer tx.Rollback()

	batchNum, err := getBatchNoByL2Block(tx, uint64(blockNumber.Int64()))
	if err != nil {
		return hexutil.Uint64(0), err
	}

	return hexutil.Uint64(batchNum), err
}

// BatchNumber returns the latest batch number
func (api *ZkEvmAPIImpl) BatchNumber(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return hexutil.Uint64(0), err
	}
	defer tx.Rollback()

	currentBatchNumber, err := getLatestBatchNumber(tx)
	if err != nil {
		return 0, err
	}

	return hexutil.Uint64(currentBatchNumber), err
}

// VirtualBatchNumber returns the latest virtual batch number
// A virtual batch is a batch that is in the process of being created and has not yet been verified.
// The virtual batch number represents the next batch to be verified using zero-knowledge proofs.
func (api *ZkEvmAPIImpl) VirtualBatchNumber(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return hexutil.Uint64(0), err
	}
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDbReader(tx)
	latestSequencedBatch, err := hermezDb.GetLatestSequence()
	if err != nil {
		return hexutil.Uint64(0), err
	}

	if latestSequencedBatch == nil {
		return hexutil.Uint64(0), nil
	}

	// todo: what if this number is the same as the last verified batch number?  do we return 0?

	return hexutil.Uint64(latestSequencedBatch.BatchNo), nil
}

// VerifiedBatchNumber returns the latest verified batch number
// A batch is considered verified once its proof has been validated and accepted by the network.
func (api *ZkEvmAPIImpl) VerifiedBatchNumber(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return hexutil.Uint64(0), err
	}
	defer tx.Rollback()

	highestVerifiedBatchNo, err := stages.GetStageProgress(tx, stages.L1VerificationsBatchNo)
	if err != nil {
		return hexutil.Uint64(0), err
	}
	return hexutil.Uint64(highestVerifiedBatchNo), nil
}

// GetBatchDataByNumbers returns the batch data for the given batch numbers
func (api *ZkEvmAPIImpl) GetBatchDataByNumbers(ctx context.Context, batchNumbers rpc.RpcNumberArray) (json.RawMessage, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDbReader(tx)

	// use inbuilt rpc.BlockNumber type to implement the 'latest' behaviour
	// highest block/batch tied to last block synced
	// unless the node is still syncing - in which case 'current block' is used
	// this is the batch number of stage progress of the Finish stage

	highestBlock, err := rawdb.ReadLastBlockSynced(tx)
	if err != nil {
		return nil, err
	}

	highestBatchNo, err := hermezDb.GetBatchNoByL2Block(highestBlock.NumberU64())
	if err != nil {
		return nil, err
	}

	// check sync status of node
	syncing, err := api.ethApi.Syncing(ctx)
	if err != nil {
		return nil, err
	}
	if syncing != nil && syncing != false {
		bn := syncing.(map[string]interface{})["currentBlock"]
		highestBatchNo, err = hermezDb.GetBatchNoByL2Block(uint64(bn.(hexutil.Uint64)))
	}

	bds := make([]*types.BatchDataSlim, 0, len(batchNumbers.Numbers))

	for _, batchNumber := range batchNumbers.Numbers {
		bd := &types.BatchDataSlim{
			Number: uint64(batchNumber.Int64()),
			Empty:  false,
		}

		// return null if we're not at this block height yet
		if batchNumber > rpc.BlockNumber(highestBatchNo) {
			bd.Empty = true
			bds = append(bds, bd)
			continue
		}

		// looks weird but we're using the rpc.BlockNumber type to represent the batch number, LatestBlockNumber represents latest batch
		if batchNumber == rpc.LatestBlockNumber {
			batchNumber = rpc.BlockNumber(highestBatchNo)
		}

		batchNo := uint64(batchNumber.Int64())

		_, found, err := hermezDb.GetLowestBlockInBatch(batchNo)
		if err != nil {
			return nil, err
		}
		if !found {
			// not found - set to empty and append
			bd.Empty = true
			bds = append(bds, bd)
			continue
		}

		// block numbers in batch
		blocksInBatch, err := hermezDb.GetL2BlockNosByBatch(batchNo)
		if err != nil {
			return nil, err
		}

		// todo: max - take out shared logic with getBatchByNumber
		// collect blocks in batch
		var batchBlocks []*eritypes.Block
		var batchTxs []eritypes.Transaction
		// handle genesis - not in the hermez tables so requires special treament
		if batchNumber == 0 {
			blk, err := api.ethApi.BaseAPI.blockByNumberWithSenders(tx, 0)
			if err != nil {
				return nil, err
			}
			batchBlocks = append(batchBlocks, blk)
			// no txs in genesis
		}
		for _, blkNo := range blocksInBatch {
			blk, err := api.ethApi.BaseAPI.blockByNumberWithSenders(tx, blkNo)
			if err != nil {
				return nil, err
			}
			batchBlocks = append(batchBlocks, blk)
			for _, btx := range blk.Transactions() {
				batchTxs = append(batchTxs, btx)
			}
		}

		// batch l2 data - must build on the fly
		forkId, err := hermezDb.GetForkId(batchNo)
		if err != nil {
			return nil, err
		}

		batchL2Data, err := generateBatchData(tx, hermezDb, batchBlocks, forkId)
		if err != nil {
			return nil, err
		}

		bd.BatchL2Data = batchL2Data
		bds = append(bds, bd)
	}

	return populateBatchDataSlimDetails(bds)
}

func generateBatchData(
	tx kv.Tx,
	hermezDb *hermez_db.HermezDbReader,
	batchBlocks []*eritypes.Block,
	forkId uint64,
) (batchL2Data []byte, err error) {
	lastBlockNoInPreviousBatch := batchBlocks[0].NumberU64() - 1
	lastBlockInPreviousBatch, err := rawdb.ReadBlockByNumber(tx, lastBlockNoInPreviousBatch)
	if err != nil {
		return nil, err
	}

	batchL2Data = []byte{}
	for i := 0; i < len(batchBlocks); i++ {
		var dTs uint32
		if i == 0 {
			dTs = uint32(batchBlocks[i].Time() - lastBlockInPreviousBatch.Time())
		} else {
			dTs = uint32(batchBlocks[i].Time() - batchBlocks[i-1].Time())
		}
		iti, err := hermezDb.GetBlockL1InfoTreeIndex(batchBlocks[i].NumberU64())
		if err != nil {
			return nil, err
		}
		egTx := make(map[common.Hash]uint8)
		for _, txn := range batchBlocks[i].Transactions() {
			eg, err := hermezDb.GetEffectiveGasPricePercentage(txn.Hash())
			if err != nil {
				return nil, err
			}
			egTx[txn.Hash()] = eg
		}

		bl2d, err := zktx.GenerateBlockBatchL2Data(uint16(forkId), dTs, uint32(iti), batchBlocks[i].Transactions(), egTx)
		if err != nil {
			return nil, err
		}
		batchL2Data = append(batchL2Data, bl2d...)
	}

	return batchL2Data, err
}

// GetBatchByNumber returns a batch from the current canonical chain. If number is nil, the
// latest known batch is returned.
func (api *ZkEvmAPIImpl) GetBatchByNumber(ctx context.Context, batchNumber rpc.BlockNumber, fullTx *bool) (json.RawMessage, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	hermezDb := hermez_db.NewHermezDbReader(tx)

	// use inbuilt rpc.BlockNumber type to implement the 'latest' behaviour
	// highest block/batch tied to last block synced
	// unless the node is still syncing - in which case 'current block' is used
	// this is the batch number of stage progress of the Finish stage

	highestBlock, err := rawdb.ReadLastBlockSynced(tx)
	if err != nil {
		return nil, err
	}

	highestBatchNo, err := hermezDb.GetBatchNoByL2Block(highestBlock.NumberU64())
	if err != nil {
		return nil, err
	}

	// check sync status of node
	syncing, err := api.ethApi.Syncing(ctx)
	if err != nil {
		return nil, err
	}
	if syncing != nil && syncing != false {
		bn := syncing.(map[string]interface{})["currentBlock"]
		highestBatchNo, err = hermezDb.GetBatchNoByL2Block(uint64(bn.(hexutil.Uint64)))
	}
	if batchNumber > rpc.BlockNumber(highestBatchNo) {
		return nil, nil
	}

	// looks weird but we're using the rpc.BlockNumber type to represent the batch number, LatestBlockNumber represents latest batch
	if batchNumber == rpc.LatestBlockNumber {
		batchNumber = rpc.BlockNumber(highestBatchNo)
	}

	batchNo := uint64(batchNumber.Int64())

	batch := &types.Batch{
		Number: types.ArgUint64(batchNo),
	}

	// mimic zkevm node null response if we don't have the batch
	_, found, err := hermezDb.GetLowestBlockInBatch(batchNo)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	// highest block in batch
	blockNo, err := hermezDb.GetHighestBlockInBatch(batchNo)
	if err != nil {
		return nil, err
	}

	block, err := api.ethApi.BaseAPI.blockByNumberWithSenders(tx, blockNo)
	if err != nil {
		return nil, err
	}

	// last block in batch data
	batch.Coinbase = block.Coinbase()
	batch.StateRoot = block.Root()

	// TODO: this logic is wrong it is the L1 verification timestamp we need
	batch.Timestamp = types.ArgUint64(block.Time())

	// block numbers in batch
	blocksInBatch, err := hermezDb.GetL2BlockNosByBatch(batchNo)
	if err != nil {
		return nil, err
	}

	// collect blocks in batch
	batch.Blocks = []interface{}{}
	batch.Transactions = []interface{}{}
	var batchBlocks []*eritypes.Block
	var batchTxs []eritypes.Transaction
	// handle genesis - not in the hermez tables so requires special treament
	if batchNumber == 0 {
		blk, err := api.ethApi.BaseAPI.blockByNumberWithSenders(tx, 0)
		if err != nil {
			return nil, err
		}
		batchBlocks = append(batchBlocks, blk)
		batch.Blocks = append(batch.Blocks, blk.Hash())
		// no txs in genesis
	}
	for _, blkNo := range blocksInBatch {
		blk, err := api.ethApi.BaseAPI.blockByNumberWithSenders(tx, blkNo)
		if err != nil {
			return nil, err
		}
		batchBlocks = append(batchBlocks, blk)
		batch.Blocks = append(batch.Blocks, blk.Hash())
		for _, btx := range blk.Transactions() {
			batchTxs = append(batchTxs, btx)
			batch.Transactions = append(batch.Transactions, btx.Hash())
		}
	}

	if fullTx != nil && *fullTx {
		batchBlocksJson := make([]interface{}, 0, len(blocksInBatch))
		batchTransactionsJson := make([]interface{}, 0, len(batchTxs))
		for _, blk := range batchBlocks {
			bbj, err := api.populateBlockDetail(tx, ctx, blk, true)
			if err != nil {
				return nil, err
			}

			bir, err := hermezDb.GetBlockInfoRoot(blk.NumberU64())
			if err != nil {
				return nil, err
			}

			ger, err := hermezDb.GetBlockGlobalExitRoot(blk.NumberU64())
			if err != nil {
				return nil, err
			}

			batchBlockExtra := &types.BlockWithInfoRootAndGer{
				Block:          &bbj,
				BlockInfoRoot:  bir,
				GlobalExitRoot: ger,
			}

			// txs
			hashes := make([]types.TransactionOrHash, len(bbj.Transactions))
			for i, txn := range bbj.Transactions {

				blkTx := blk.Transactions()[i]
				l2TxHash, err := zktx.ComputeL2TxHash(
					blkTx.GetChainID().ToBig(),
					blkTx.GetValue(),
					blkTx.GetPrice(),
					blkTx.GetNonce(),
					blkTx.GetGas(),
					blkTx.GetTo(),
					&txn.Tx.From,
					blkTx.GetData(),
				)
				if err != nil {
					return nil, err
				}

				txn.Tx.L2Hash = l2TxHash
				txn.Tx.Receipt.TransactionL2Hash = l2TxHash

				batchTransactionsJson = append(batchTransactionsJson, txn)
				txn.Hash = &txn.Tx.Hash
				txn.Tx = nil
				hashes[i] = txn
			}

			// after collecting transactions, reduce them to them hash only on the block
			bbj.Transactions = hashes

			batchBlocksJson = append(batchBlocksJson, batchBlockExtra)
		}

		batch.Blocks = batchBlocksJson
		batch.Transactions = batchTransactionsJson
	}

	// for consistency with legacy node, return nil if no transactions
	if len(batch.Transactions) == 0 {
		batch.Transactions = nil
	}

	// global exit root of batch
	batchGer, _, err := hermezDb.GetLastBlockGlobalExitRoot(blockNo)
	if err != nil {
		return nil, err
	}

	batch.GlobalExitRoot = batchGer

	// sequence
	seq, err := hermezDb.GetSequenceByBatchNo(batchNo)
	if err != nil {
		return nil, err
	}
	if seq != nil {
		batch.SendSequencesTxHash = &seq.L1TxHash
	}
	_, found, err = hermezDb.GetLowestBlockInBatch(batchNo + 1)
	if err != nil {
		return nil, err
	}
	// sequenced, genesis or injected batch 1 - special batches 0,1 will always be closed, if next batch has blocks, bn must be closed
	batch.Closed = seq != nil || batchNo == 0 || batchNo == 1 || found

	// verification
	ver, err := hermezDb.GetVerificationByBatchNo(batchNo)
	if err != nil {
		return nil, err
	}
	if ver != nil {
		batch.VerifyBatchTxHash = &ver.L1TxHash
	}

	// exit roots (MainnetExitRoot, RollupExitRoot)
	infoTreeUpdate, err := hermezDb.GetL1InfoTreeUpdateByGer(batchGer)
	if err != nil {
		return nil, err
	}
	if infoTreeUpdate != nil {
		batch.MainnetExitRoot = infoTreeUpdate.MainnetExitRoot
		batch.RollupExitRoot = infoTreeUpdate.RollupExitRoot
	}

	// local exit root
	localExitRoot, err := utils.GetBatchLocalExitRoot(batchNo, hermezDb, tx)
	if err != nil {
		return nil, err
	}
	batch.LocalExitRoot = localExitRoot

	// batch l2 data - must build on the fly
	forkId, err := hermezDb.GetForkId(batchNo)
	if err != nil {
		return nil, err
	}

	batchL2Data, err := generateBatchData(tx, hermezDb, batchBlocks, forkId)
	if err != nil {
		return nil, err
	}
	batch.BatchL2Data = batchL2Data

	oldAccInputHash, err := api.l1Syncer.GetOldAccInputHash(ctx, &api.config.AddressRollup, ApiRollupId, batchNo)
	if err != nil {
		return nil, err
	}
	batch.AccInputHash = oldAccInputHash

	// forkid exit roots logic
	// if forkid < 12 then we should only set the exit roots if they have changed, otherwise 0x00..00
	// if forkid >= 12 then we should always set the exit roots
	if forkId < 12 {
		// get the previous batches exit roots
		prevBatchNo := batchNo - 1
		prevBatchHighestBlock, err := hermezDb.GetHighestBlockInBatch(prevBatchNo)
		if err != nil {
			return nil, err
		}
		prevBatchGer, _, err := hermezDb.GetLastBlockGlobalExitRoot(prevBatchHighestBlock)
		if err != nil {
			return nil, err
		}

		itu, err := hermezDb.GetL1InfoTreeUpdateByGer(prevBatchGer)
		if err != nil {
			return nil, err
		}

		if itu == nil || batch.MainnetExitRoot == itu.MainnetExitRoot {
			batch.MainnetExitRoot = common.Hash{}
			batch.RollupExitRoot = common.Hash{}
			batch.GlobalExitRoot = common.Hash{}
		}
	}

	return populateBatchDetails(batch)
}

// GetFullBlockByNumber returns a full block from the current canonical chain. If number is nil, the
// latest known block is returned.
func (api *ZkEvmAPIImpl) GetFullBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (types.Block, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return types.Block{}, err
	}
	defer tx.Rollback()

	baseBlock, err := api.ethApi.BaseAPI.blockByRPCNumber(number, tx)
	if err != nil {
		return types.Block{}, err
	}
	if baseBlock == nil {
		return types.Block{}, errors.New("could not find block")
	}

	return api.populateBlockDetail(tx, ctx, baseBlock, fullTx)
}

// GetFullBlockByHash returns a full block from the current canonical chain. If number is nil, the
// latest known block is returned.
func (api *ZkEvmAPIImpl) GetFullBlockByHash(ctx context.Context, hash libcommon.Hash, fullTx bool) (types.Block, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return types.Block{}, err
	}
	defer tx.Rollback()

	baseBlock, err := api.ethApi.BaseAPI.blockByHashWithSenders(tx, hash)
	if err != nil {
		return types.Block{}, err
	}
	if baseBlock == nil {
		return types.Block{}, fmt.Errorf("block not found")
	}

	return api.populateBlockDetail(tx, ctx, baseBlock, fullTx)
}

// zkevm_getExitRootsByGER returns the exit roots accordingly to the provided Global Exit Root
func (api *ZkEvmAPIImpl) GetExitRootsByGER(ctx context.Context, globalExitRoot common.Hash) (*ZkExitRoots, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDbReader(tx)
	infoTreeUpdate, err := hermezDb.GetL1InfoTreeUpdateByGer(globalExitRoot)
	if err != nil {
		return nil, err
	}

	if infoTreeUpdate == nil {
		return nil, nil
	}

	return &ZkExitRoots{
		BlockNumber:     types.ArgUint64(infoTreeUpdate.BlockNumber),
		Timestamp:       types.ArgUint64(infoTreeUpdate.Timestamp),
		MainnetExitRoot: infoTreeUpdate.MainnetExitRoot,
		RollupExitRoot:  infoTreeUpdate.RollupExitRoot,
	}, nil
}

func (api *ZkEvmAPIImpl) populateBlockDetail(
	tx kv.Tx,
	ctx context.Context,
	baseBlock *eritypes.Block,
	fullTx bool,
) (types.Block, error) {
	cc, err := api.ethApi.chainConfig(tx)
	if err != nil {
		return types.Block{}, err
	}

	// doing this here seems stragne, and it is.  But because we change the header hash in execution
	// to populate details we don't have in the batches stage, the senders are held against the wrong hash.
	// the call later to `getReceipts` sets the incorrect sender because of this so we need to calc and hold
	// these ahead of time.  TODO: fix senders stage to avoid this or update them with the new hash in execution
	number := baseBlock.NumberU64()
	hermezReader := hermez_db.NewHermezDbReader(tx)

	sendersFixed := false
	var senders []common.Address
	var signer *eritypes.Signer
	if sendersFixed {
		senders = baseBlock.Body().SendersFromTxs()
	} else {
		signer = eritypes.MakeSigner(cc, number)
	}

	var effectiveGasPricePercentages []uint8
	if fullTx {
		for _, txn := range baseBlock.Transactions() {
			if signer != nil {
				sender, err := txn.Sender(*signer)
				if err != nil {
					return types.Block{}, err
				}
				senders = append(senders, sender)
			}
			effectiveGasPricePercentage, err := hermezReader.GetEffectiveGasPricePercentage(txn.Hash())
			if err != nil {
				return types.Block{}, err
			}
			effectiveGasPricePercentages = append(effectiveGasPricePercentages, effectiveGasPricePercentage)
		}
	}

	receipts, err := api.ethApi.BaseAPI.getReceipts(ctx, tx, cc, baseBlock, baseBlock.Body().SendersFromTxs())
	if err != nil {
		return types.Block{}, err
	}

	return convertBlockToRpcBlock(baseBlock, receipts, senders, effectiveGasPricePercentages, fullTx)
}

// GetBroadcastURI returns the URI of the broadcaster - the trusted sequencer
// func (api *ZkEvmAPIImpl) GetBroadcastURI(ctx context.Context) (string, error) {
// 	return api.ethApi.ZkRpcUrl, nil
// }

func (api *ZkEvmAPIImpl) GetWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, mode *WitnessMode, debug *bool) (hexutility.Bytes, error) {
	checkedMode := WitnessModeNone
	if mode != nil && *mode != WitnessModeFull && *mode != WitnessModeTrimmed {
		return nil, errors.New("invalid mode, must be full or trimmed")
	} else if mode != nil {
		checkedMode = *mode
	}

	dbg := false
	if debug != nil {
		dbg = *debug
	}
	return api.getBlockRangeWitness(ctx, api.db, blockNrOrHash, blockNrOrHash, dbg, checkedMode)
}

func (api *ZkEvmAPIImpl) GetBlockRangeWitness(ctx context.Context, startBlockNrOrHash rpc.BlockNumberOrHash, endBlockNrOrHash rpc.BlockNumberOrHash, mode *WitnessMode, debug *bool) (hexutility.Bytes, error) {
	checkedMode := WitnessModeNone
	if mode != nil && *mode != WitnessModeFull && *mode != WitnessModeTrimmed {
		return nil, errors.New("invalid mode, must be full or trimmed")
	} else if mode != nil {
		checkedMode = *mode
	}

	dbg := false
	if debug != nil {
		dbg = *debug
	}
	return api.getBlockRangeWitness(ctx, api.db, startBlockNrOrHash, endBlockNrOrHash, dbg, checkedMode)
}

func (api *ZkEvmAPIImpl) getBatchWitness(ctx context.Context, tx kv.Tx, batchNum uint64, debug bool, mode WitnessMode) (hexutility.Bytes, error) {
	if api.ethApi.historyV3(tx) {
		return nil, fmt.Errorf("not supported by Erigon3")
	}

	generator, fullWitness, err := api.buildGenerator(tx, mode)
	if err != nil {
		return nil, err
	}

	return generator.GetWitnessByBatch(tx, ctx, batchNum, debug, fullWitness)

}

func (api *ZkEvmAPIImpl) buildGenerator(tx kv.Tx, witnessMode WitnessMode) (*witness.Generator, bool, error) {
	chainConfig, err := api.ethApi.chainConfig(tx)
	if err != nil {
		return nil, false, err
	}

	generator := witness.NewGenerator(
		api.ethApi.dirs,
		api.ethApi.historyV3(tx),
		api.ethApi._agg,
		api.ethApi._blockReader,
		chainConfig,
		api.ethApi._engine,
	)

	fullWitness := false
	if witnessMode == WitnessModeNone {
		fullWitness = api.config.WitnessFull
	} else if witnessMode == WitnessModeFull {
		fullWitness = true
	}

	return generator, fullWitness, nil
}

// Get witness for a range of blocks [startBlockNrOrHash, endBlockNrOrHash] (inclusive)
func (api *ZkEvmAPIImpl) getBlockRangeWitness(ctx context.Context, db kv.RoDB, startBlockNrOrHash rpc.BlockNumberOrHash, endBlockNrOrHash rpc.BlockNumberOrHash, debug bool, witnessMode WitnessMode) (hexutility.Bytes, error) {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if api.ethApi.historyV3(tx) {
		return nil, fmt.Errorf("not supported by Erigon3")
	}

	blockNr, _, _, err := rpchelper.GetCanonicalBlockNumber(startBlockNrOrHash, tx, api.ethApi.filters) // DoCall cannot be executed on non-canonical blocks
	if err != nil {
		return nil, err
	}

	endBlockNr, _, _, err := rpchelper.GetCanonicalBlockNumber(endBlockNrOrHash, tx, api.ethApi.filters) // DoCall cannot be executed on non-canonical blocks

	if err != nil {
		return nil, err
	}

	if blockNr > endBlockNr {
		return nil, fmt.Errorf("start block number must be less than or equal to end block number, start=%d end=%d", blockNr, endBlockNr)
	}

	generator, fullWitness, err := api.buildGenerator(tx, witnessMode)
	if err != nil {
		return nil, err
	}

	return generator.GetWitnessByBlockRange(tx, ctx, blockNr, endBlockNr, debug, fullWitness)
}

type WitnessMode string

const (
	WitnessModeNone    WitnessMode = "none"
	WitnessModeFull    WitnessMode = "full"
	WitnessModeTrimmed WitnessMode = "trimmed"
)

func (api *ZkEvmAPIImpl) GetBatchWitness(ctx context.Context, batchNumber uint64, mode *WitnessMode) (interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDbReader(tx)
	badBatch, err := hermezDb.GetInvalidBatch(batchNumber)
	if err != nil {
		return nil, err
	}

	if badBatch && !sequencer.IsSequencer() {
		// we won't have the details in our db if the batch is marked as invalid so we need to check this
		// here
		return api.sendGetBatchWitness(api.l2SequencerUrl, batchNumber, mode)
	}

	checkedMode := WitnessModeNone
	if mode != nil && *mode != WitnessModeFull && *mode != WitnessModeTrimmed {
		return nil, errors.New("invalid mode, must be full or trimmed")
	} else if mode != nil {
		checkedMode = *mode
	}

	// we only want to check the cache if no special run mode has been supplied.  If a run mode is supplied
	// we need to always regenerate the witness from scratch
	if checkedMode == WitnessModeNone {
		hermezDb := hermez_db.NewHermezDbReader(tx)
		witnessCached, err := hermezDb.GetWitness(batchNumber)
		if err != nil {
			return nil, err
		}
		if witnessCached != nil {
			return witnessCached, nil
		}
	}

	return api.getBatchWitness(ctx, tx, batchNumber, false, checkedMode)
}

func (api *ZkEvmAPIImpl) GetProverInput(ctx context.Context, batchNumber uint64, mode *WitnessMode, debug *bool) (*legacy_executor_verifier.RpcPayload, error) {
	if !sequencer.IsSequencer() {
		return nil, errors.New("method only supported from a sequencer node")
	}

	checkedMode := WitnessModeNone
	if mode != nil && *mode != WitnessModeFull && *mode != WitnessModeTrimmed {
		return nil, errors.New("invalid mode, must be full or trimmed")
	} else if mode != nil {
		checkedMode = *mode
	}

	useDebug := false
	if debug != nil {
		useDebug = *debug
	}

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	hDb := hermez_db.NewHermezDbReader(tx)

	blockNumbers, err := hDb.GetL2BlockNosByBatch(batchNumber)
	if err != nil {
		return nil, err
	}

	lastBlock, err := rawdb.ReadBlockByNumber(tx, blockNumbers[len(blockNumbers)-1])
	if err != nil {
		return nil, err
	}

	start := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNumbers[0]))
	end := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNumbers[len(blockNumbers)-1]))

	rangeWitness, err := api.getBlockRangeWitness(ctx, api.db, start, end, useDebug, checkedMode)
	if err != nil {
		return nil, err
	}

	oldAccInputHash, err := api.l1Syncer.GetOldAccInputHash(ctx, &api.config.AddressRollup, ApiRollupId, batchNumber)
	if err != nil {
		return nil, err
	}

	timestampLimit := lastBlock.Time()

	return &legacy_executor_verifier.RpcPayload{
		Witness:           hex.EncodeToHex(rangeWitness),
		Coinbase:          api.config.AddressSequencer.String(),
		OldAccInputHash:   oldAccInputHash.String(),
		TimestampLimit:    timestampLimit,
		ForcedBlockhashL1: "",
	}, nil
}

func (api *ZkEvmAPIImpl) GetLatestGlobalExitRoot(ctx context.Context) (common.Hash, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return common.Hash{}, err
	}
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDbReader(tx)
	_, ger, err := hermezDb.GetLatestUsedGer()
	if err != nil {
		return common.Hash{}, err
	}

	return ger, nil
}

type l1InfoTreeData struct {
	Index           uint64      `json:"index"`
	Ger             common.Hash `json:"ger"`
	InfoRoot        common.Hash `json:"info_root"`
	MainnetExitRoot common.Hash `json:"mainnet_exit_root"`
	RollupExitRoot  common.Hash `json:"rollup_exit_root"`
	ParentHash      common.Hash `json:"parent_hash"`
	MinTimestamp    uint64      `json:"min_timestamp"`
	BlockNumber     uint64      `json:"block_number"`
}

func (api *ZkEvmAPIImpl) GetExitRootTable(ctx context.Context) ([]l1InfoTreeData, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDbReader(tx)

	indexToRoots, err := hermezDb.GetL1InfoTreeIndexToRoots()
	if err != nil {
		return nil, err
	}

	var result []l1InfoTreeData

	var idx uint64 = 1
	for {
		info, err := hermezDb.GetL1InfoTreeUpdate(idx)
		if err != nil {
			return nil, err
		}
		if info == nil || info.Index == 0 {
			break
		}
		data := l1InfoTreeData{
			Index:           info.Index,
			Ger:             info.GER,
			MainnetExitRoot: info.MainnetExitRoot,
			RollupExitRoot:  info.RollupExitRoot,
			ParentHash:      info.ParentHash,
			MinTimestamp:    info.Timestamp,
			BlockNumber:     info.BlockNumber,
			InfoRoot:        indexToRoots[info.Index],
		}
		result = append(result, data)
		idx++
	}

	return result, nil
}

func (api *ZkEvmAPIImpl) sendGetBatchWitness(rpcUrl string, batchNumber uint64, mode *WitnessMode) (json.RawMessage, error) {
	res, err := client.JSONRPCCall(rpcUrl, "zkevm_getBatchWitness", batchNumber, mode)
	if err != nil {
		return nil, err
	}

	return res.Result, nil
}

func getLastBlockInBatchNumber(tx kv.Tx, batchNumber uint64) (uint64, error) {
	reader := hermez_db.NewHermezDbReader(tx)

	if batchNumber == 0 {
		return 0, nil
	}

	blocks, err := reader.GetL2BlockNosByBatch(batchNumber)
	if err != nil {
		return 0, err
	}
	return blocks[len(blocks)-1], nil
}

func getAllBlocksInBatchNumber(tx kv.Tx, batchNumber uint64) ([]uint64, error) {
	reader := hermez_db.NewHermezDbReader(tx)
	return reader.GetL2BlockNosByBatch(batchNumber)
}

func getLatestBatchNumber(tx kv.Tx) (uint64, error) {
	c, err := tx.Cursor(hermez_db.BLOCKBATCHES)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	// get the last entry from the table
	k, v, err := c.Last()
	if err != nil {
		return 0, err
	}
	if k == nil {
		return 0, nil
	}

	return hermez_db.BytesToUint64(v), nil
}

func getBatchNoByL2Block(tx kv.Tx, l2BlockNo uint64) (uint64, error) {
	reader := hermez_db.NewHermezDbReader(tx)
	return reader.GetBatchNoByL2Block(l2BlockNo)
}

func convertTransactionsReceipts(
	txs []eritypes.Transaction,
	receipts eritypes.Receipts,
	hermezReader hermez_db.HermezDbReader,
	block eritypes.Block) ([]types.Transaction, error) {
	if len(txs) != len(receipts) {
		return nil, errors.New("transactions and receipts length mismatch")
	}

	result := make([]types.Transaction, 0, len(txs))

	for idx, tx := range txs {
		effectiveGasPricePercentage, err := hermezReader.GetEffectiveGasPricePercentage(tx.Hash())
		if err != nil {
			return nil, err
		}
		gasPrice := tx.GetPrice()
		v, r, s := tx.RawSignatureValues()
		var sender common.Address

		// TODO: senders!

		var receipt *types.Receipt
		if len(receipts) > idx {
			receipt = convertReceipt(receipts[idx], sender, tx.GetTo(), gasPrice, effectiveGasPricePercentage)
		}

		bh := block.Hash()
		blockNumber := block.NumberU64()

		tran := types.Transaction{
			Nonce:       types.ArgUint64(tx.GetNonce()),
			GasPrice:    types.ArgBig(*gasPrice.ToBig()),
			Gas:         types.ArgUint64(tx.GetGas()),
			To:          tx.GetTo(),
			Value:       types.ArgBig(*tx.GetValue().ToBig()),
			Input:       tx.GetData(),
			V:           types.ArgBig(*v.ToBig()),
			R:           types.ArgBig(*r.ToBig()),
			S:           types.ArgBig(*s.ToBig()),
			Hash:        tx.Hash(),
			From:        sender,
			BlockHash:   &bh,
			BlockNumber: types.ArgUint64Ptr(types.ArgUint64(blockNumber)),
			TxIndex:     types.ArgUint64Ptr(types.ArgUint64(idx)),
			Type:        types.ArgUint64(tx.Type()),
			Receipt:     receipt,
		}

		cid := tx.GetChainID()
		var cidAB *types.ArgBig
		if cid.Cmp(uint256.NewInt(0)) != 0 {
			cidAB = (*types.ArgBig)(cid.ToBig())
			tran.ChainID = cidAB
		}

		result = append(result, tran)
	}

	return result, nil
}

func convertBlockToRpcBlock(
	orig *eritypes.Block,
	receipts eritypes.Receipts,
	senders []common.Address,
	effectiveGasPricePercentages []uint8,
	full bool,
) (types.Block, error) {
	header := orig.Header()

	var difficulty uint64
	if header.Difficulty != nil {
		difficulty = header.Difficulty.Uint64()
	} else {
		difficulty = uint64(0)
	}

	n := big.NewInt(0).SetUint64(header.Nonce.Uint64())
	nonce := types.LeftPadBytes(n.Bytes(), 8) //nolint:gomnd
	blockHash := orig.Hash()
	blockNumber := orig.NumberU64()

	result := types.Block{
		ParentHash:      header.ParentHash,
		Sha3Uncles:      sha3UncleHash,
		Miner:           header.Coinbase,
		StateRoot:       header.Root,
		TxRoot:          header.TxHash,
		ReceiptsRoot:    header.ReceiptHash,
		LogsBloom:       header.Bloom,
		Difficulty:      types.ArgUint64(difficulty),
		TotalDifficulty: types.ArgUint64(difficulty),
		Size:            types.ArgUint64(orig.Size()),
		Number:          types.ArgUint64(blockNumber),
		GasLimit:        types.ArgUint64(header.GasLimit),
		GasUsed:         types.ArgUint64(header.GasUsed),
		Timestamp:       types.ArgUint64(header.Time),
		ExtraData:       types.ArgBytes(header.Extra),
		MixHash:         header.MixDigest,
		Nonce:           nonce,
		Hash:            blockHash,
		Transactions:    []types.TransactionOrHash{},
		Uncles:          []common.Hash{},
	}

	if full {
		for idx, tx := range orig.Transactions() {
			gasPrice := tx.GetPrice()
			v, r, s := tx.RawSignatureValues()
			var sender common.Address
			if len(senders) > idx {
				sender = senders[idx]
			}
			var effectiveGasPricePercentage uint8 = 0
			if len(effectiveGasPricePercentages) > idx {
				effectiveGasPricePercentage = effectiveGasPricePercentages[idx]
			}
			var receipt *types.Receipt
			if len(receipts) > idx {
				receipt = convertReceipt(receipts[idx], sender, tx.GetTo(), gasPrice, effectiveGasPricePercentage)
			}

			tran := types.Transaction{
				Nonce:       types.ArgUint64(tx.GetNonce()),
				GasPrice:    types.ArgBig(*gasPrice.ToBig()),
				Gas:         types.ArgUint64(tx.GetGas()),
				To:          tx.GetTo(),
				Value:       types.ArgBig(*tx.GetValue().ToBig()),
				Input:       tx.GetData(),
				V:           types.ArgBig(*v.ToBig()),
				R:           types.ArgBig(*r.ToBig()),
				S:           types.ArgBig(*s.ToBig()),
				Hash:        tx.Hash(),
				From:        sender,
				BlockHash:   &blockHash,
				BlockNumber: types.ArgUint64Ptr(types.ArgUint64(blockNumber)),
				TxIndex:     types.ArgUint64Ptr(types.ArgUint64(idx)),
				Type:        types.ArgUint64(tx.Type()),
				Receipt:     receipt,
			}

			cid := tx.GetChainID()
			if cid.Cmp(uint256.NewInt(0)) != 0 {
				tran.ChainID = (*types.ArgBig)(cid.ToBig())
			}

			t := types.TransactionOrHash{Tx: &tran}
			result.Transactions = append(result.Transactions, t)
		}
	} else {
		for _, tx := range orig.Transactions() {
			h := tx.Hash()
			th := types.TransactionOrHash{Hash: &h}
			result.Transactions = append(result.Transactions, th)
		}
	}

	return result, nil
}

func convertReceipt(
	r *eritypes.Receipt,
	from common.Address,
	to *common.Address,
	gasPrice *uint256.Int,
	effectiveGasPricePercentage uint8,
) *types.Receipt {
	var cAddr *common.Address
	if r.ContractAddress != (common.Address{}) {
		cAddr = &r.ContractAddress
	}

	// ensure logs is always an empty array rather than nil in the response
	logs := make([]*eritypes.Log, 0)
	if len(r.Logs) > 0 {
		logs = r.Logs
	}

	var effectiveGasPrice *types.ArgBig
	if gasPrice != nil {
		gas := core.CalculateEffectiveGas(gasPrice.Clone(), effectiveGasPricePercentage)
		asBig := types.ArgBig(*gas.ToBig())
		effectiveGasPrice = &asBig
	}

	return &types.Receipt{
		CumulativeGasUsed: types.ArgUint64(r.CumulativeGasUsed),
		LogsBloom:         eritypes.CreateBloom(eritypes.Receipts{r}),
		Logs:              logs,
		Status:            types.ArgUint64(r.Status),
		TxHash:            r.TxHash,
		TxIndex:           types.ArgUint64(r.TransactionIndex),
		BlockHash:         r.BlockHash,
		BlockNumber:       types.ArgUint64(r.BlockNumber.Uint64()),
		GasUsed:           types.ArgUint64(r.GasUsed),
		FromAddr:          from,
		ToAddr:            to,
		ContractAddress:   cAddr,
		Type:              types.ArgUint64(r.Type),
		EffectiveGasPrice: effectiveGasPrice,
	}
}

func populateBatchDetails(batch *types.Batch) (json.RawMessage, error) {
	jBatch := map[string]interface{}{}
	jBatch["number"] = batch.Number
	jBatch["coinbase"] = batch.Coinbase
	jBatch["stateRoot"] = batch.StateRoot
	jBatch["timestamp"] = batch.Timestamp
	jBatch["blocks"] = batch.Blocks
	jBatch["transactions"] = batch.Transactions
	jBatch["globalExitRoot"] = batch.GlobalExitRoot
	jBatch["mainnetExitRoot"] = batch.MainnetExitRoot
	jBatch["rollupExitRoot"] = batch.RollupExitRoot
	jBatch["localExitRoot"] = batch.LocalExitRoot
	jBatch["sendSequencesTxHash"] = batch.SendSequencesTxHash
	jBatch["verifyBatchTxHash"] = batch.VerifyBatchTxHash

	if batch.ForcedBatchNumber != nil {
		jBatch["forcedBatchNumber"] = batch.ForcedBatchNumber
	}
	if batch.AccInputHash != (common.Hash{}) {
		jBatch["accInputHash"] = batch.AccInputHash
	}
	jBatch["closed"] = batch.Closed
	if len(batch.BatchL2Data) > 0 {
		jBatch["batchL2Data"] = batch.BatchL2Data
	}

	return json.Marshal(jBatch)
}

func populateBatchDataSlimDetails(batches []*types.BatchDataSlim) (json.RawMessage, error) {
	jBatches := make([]map[string]interface{}, 0, len(batches))
	for _, b := range batches {
		jBatch := map[string]interface{}{}
		jBatch["number"] = b.Number
		jBatch["empty"] = b.Empty
		if !b.Empty {
			jBatch["batchL2Data"] = b.BatchL2Data
		}
		jBatches = append(jBatches, jBatch)
	}

	return json.Marshal(jBatches)
}
