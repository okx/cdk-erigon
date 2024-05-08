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

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	types "github.com/ledgerwatch/erigon/zk/rpcdaemon"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	"github.com/ledgerwatch/erigon/zk/syncer"
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
	GetBatchWitness(ctx context.Context, batchNumber uint64, mode *WitnessMode) (hexutility.Bytes, error)
	GetProverInput(ctx context.Context, batchNumber uint64, mode *WitnessMode, debug *bool) (*legacy_executor_verifier.RpcPayload, error)
	GetLatestGlobalExitRoot(ctx context.Context) (common.Hash, error)
	GetExitRootsByGER(ctx context.Context, globalExitRoot common.Hash) (*ZkExitRoots, error)
}

// APIImpl is implementation of the ZkEvmAPI interface based on remote Db access
type ZkEvmAPIImpl struct {
	ethApi *APIImpl

	db              kv.RoDB
	ReturnDataLimit int
	config          *ethconfig.Config
	l1Syncer        *syncer.L1Syncer
}

// NewEthAPI returns ZkEvmAPIImpl instance
func NewZkEvmAPI(
	base *APIImpl,
	db kv.RoDB,
	returnDataLimit int,
	zkConfig *ethconfig.Config,
	l1Syncer *syncer.L1Syncer,
) *ZkEvmAPIImpl {
	return &ZkEvmAPIImpl{
		ethApi:          base,
		db:              db,
		ReturnDataLimit: returnDataLimit,
		config:          zkConfig,
		l1Syncer:        l1Syncer,
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

// GetBatchByNumber returns a batch from the current canonical chain. If number is nil, the
// latest known batch is returned.
func (api *ZkEvmAPIImpl) GetBatchByNumber(ctx context.Context, batchNumber rpc.BlockNumber, fullTx *bool) (json.RawMessage, error) {
	res, err := client.JSONRPCCall(api.ethApi.ZkRpcUrl, "zkevm_getBatchByNumber", batchNumber, fullTx)
	if err != nil {
		return nil, err
	}

	return res.Result, nil

	// TODO: implement this when we get to sequencer activities in erigon
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
	signer := eritypes.MakeSigner(cc, number)
	var senders []common.Address
	var effectiveGasPricePercentages []uint8
	if fullTx {
		for _, txn := range baseBlock.Transactions() {
			sender, err := txn.Sender(*signer)
			if err != nil {
				return types.Block{}, err
			}
			senders = append(senders, sender)
			effectiveGasPricePercentage, err := api.ethApi.getEffectiveGasPricePercentage(tx, txn.Hash())
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

	chainConfig, err := api.ethApi.chainConfig(tx)
	if err != nil {
		return nil, err
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

	return generator.GenerateWitness(tx, ctx, blockNr, endBlockNr, debug, fullWitness)
}

type WitnessMode string

const (
	WitnessModeNone    WitnessMode = "none"
	WitnessModeFull    WitnessMode = "full"
	WitnessModeTrimmed WitnessMode = "trimmed"
)

func (api *ZkEvmAPIImpl) GetBatchWitness(ctx context.Context, batchNumber uint64, mode *WitnessMode) (hexutility.Bytes, error) {
	checkedMode := WitnessModeNone
	if mode != nil && *mode != WitnessModeFull && *mode != WitnessModeTrimmed {
		return nil, errors.New("invalid mode, must be full or trimmed")
	} else if mode != nil {
		checkedMode = *mode
	}

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

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

	blocks, err := getAllBlocksInBatchNumber(tx, batchNumber)

	if err != nil {
		return nil, err
	}

	if len(blocks) == 0 {
		return nil, errors.New("batch not found")
	}

	endBlock := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blocks[0]))
	startBlock := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blocks[len(blocks)-1]))
	return api.getBlockRangeWitness(ctx, api.db, startBlock, endBlock, false, checkedMode)
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

func getLastBlockInBatchNumber(tx kv.Tx, batchNumber uint64) (uint64, error) {
	c, err := tx.Cursor(hermez_db.BLOCKBATCHES)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	var k, v []byte
	for k, v, err = c.Last(); k != nil; k, v, err = c.Prev() {
		if err != nil {
			return 0, err
		}
		val := hermez_db.BytesToUint64(v)
		if val == batchNumber {
			return hermez_db.BytesToUint64(k), nil
		}
	}

	return 0, nil
}

func getAllBlocksInBatchNumber(tx kv.Tx, batchNumber uint64) ([]uint64, error) {
	c, err := tx.Cursor(hermez_db.BLOCKBATCHES)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	result := make([]uint64, 0)
	var k, v []byte
	inScope := false
	for k, v, err = c.Last(); k != nil; k, v, err = c.Prev() {
		if err != nil {
			return nil, err
		}
		val := hermez_db.BytesToUint64(v)
		if val == batchNumber {
			inScope = true
			result = append(result, hermez_db.BytesToUint64(k))
		} else {
			if inScope {
				// reached the end of block of batches
				break
			}
		}
	}

	return result, nil
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
	c, err := tx.Cursor(hermez_db.BLOCKBATCHES)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	k, v, err := c.Seek(hermez_db.Uint64ToBytes(l2BlockNo))
	if err != nil {
		return 0, err
	}

	if k == nil {
		return 0, nil
	}

	if hermez_db.BytesToUint64(k) != l2BlockNo {
		return 0, nil
	}

	return hermez_db.BytesToUint64(v), nil
}

func getGlobalExitRoot(tx kv.Tx, l2Block uint64) (common.Hash, error) {
	d, err := tx.GetOne(hermez_db.BLOCK_GLOBAL_EXIT_ROOTS, hermez_db.Uint64ToBytes(l2Block))
	if err != nil {
		return common.Hash{}, err
	}
	return common.BytesToHash(d), nil
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
				ChainID:     types.ArgBig(*tx.GetChainID().ToBig()),
				Type:        types.ArgUint64(tx.Type()),
				Receipt:     receipt,
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
