package server

import (
	"fmt"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/erigon/zk/utils"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
)

type DbReader interface {
	GetL2BlockNosByBatch(batchNo uint64) ([]uint64, error)
	GetBatchGlobalExitRootsProto(lastBatchNumber, batchNumber uint64) ([]types.GerUpdateProto, error)
	GetForkId(batchNumber uint64) (uint64, error)
	GetBlockGlobalExitRoot(blockNumber uint64) (libcommon.Hash, error)
	GetBlockL1BlockHash(blockNumber uint64) (libcommon.Hash, error)
	GetBlockL1InfoTreeIndex(blockNumber uint64) (uint64, error)
	GetL1InfoTreeUpdate(index uint64) (*zktypes.L1InfoTreeUpdate, error)
	GetBlockInfoRoot(blockNumber uint64) (libcommon.Hash, error)
	GetIntermediateTxStateRoot(blockNumber uint64, txHash libcommon.Hash) (libcommon.Hash, error)
	GetEffectiveGasPricePercentage(txHash libcommon.Hash) (uint8, error)
	GetHighestBlockInBatch(batchNumber uint64) (uint64, error)
	GetInvalidBatch(batchNumber uint64) (bool, error)
	GetBatchNoByL2Block(blockNumber uint64) (uint64, error)
	CheckBatchNoByL2Block(l2BlockNo uint64) (uint64, bool, error)
}

type BookmarkType byte

const (
	EtrogBatchNumber = 7
)

type DataStreamServer struct {
	stream  *datastreamer.StreamServer
	chainId uint64
	highestBlockWritten,
	highestClosedBatchWritten,
	highestBatchWritten *uint64
}

type DataStreamEntry interface {
	EntryType() types.EntryType
	Bytes(bigEndian bool) []byte
}

type DataStreamEntryProto interface {
	Marshal() ([]byte, error)
	Type() types.EntryType
}

func NewDataStreamServer(stream *datastreamer.StreamServer, chainId uint64) *DataStreamServer {
	return &DataStreamServer{
		stream:              stream,
		chainId:             chainId,
		highestBlockWritten: nil,
		highestBatchWritten: nil,
	}
}

func (srv *DataStreamServer) GetChainId() uint64 {
	return srv.chainId
}

type DataStreamEntries struct {
	index   int
	entries []DataStreamEntryProto
}

func (d *DataStreamEntries) Add(entry DataStreamEntryProto) {
	d.entries[d.index] = entry
	d.index++
}

func (d *DataStreamEntries) AddMany(entries []DataStreamEntryProto) {
	for _, e := range entries {
		d.Add(e)
	}
}

func (d *DataStreamEntries) Size() int {
	if d == nil || d.entries == nil {
		return 0
	}
	return len(d.entries)
}

func (d *DataStreamEntries) Entries() []DataStreamEntryProto {
	if d == nil || d.entries == nil {
		return []DataStreamEntryProto{}
	}
	return d.entries
}

func (d *DataStreamEntries) Marshal() (result []byte, err error) {
	var b []byte
	for _, entry := range d.entries {
		b, err = encodeEntryToBytesProto(entry)
		if err != nil {
			return nil, err
		}
		result = append(result, b...)
	}

	return result, nil
}

func NewDataStreamEntries(size int) *DataStreamEntries {
	return &DataStreamEntries{
		entries: make([]DataStreamEntryProto, size),
	}
}

func (srv *DataStreamServer) commitAtomicOp(latestBlockNum, latestBatchNum, latestClosedBatch *uint64) error {
	if err := srv.stream.CommitAtomicOp(); err != nil {
		return err
	}

	// copy the values in case they are changed outside the function
	// pointers are used for easier check if we should set check them from the DS or not
	// since 0 is a valid number, we can't use it
	if latestBlockNum != nil {
		a := *latestBlockNum
		srv.highestBlockWritten = &a
	}

	if latestBatchNum != nil {
		a := *latestBatchNum
		srv.highestBatchWritten = &a
	}

	if latestClosedBatch != nil {
		a := *latestClosedBatch
		srv.highestClosedBatchWritten = &a
	}

	return nil
}

func (srv *DataStreamServer) commitEntriesToStreamProto(entries []DataStreamEntryProto) error {
	for _, entry := range entries {
		entryType := entry.Type()

		em, err := entry.Marshal()
		if err != nil {
			return err
		}

		if entryType == types.BookmarkEntryType {
			if _, err = srv.stream.AddStreamBookmark(em); err != nil {
				return err
			}
		} else {
			if _, err = srv.stream.AddStreamEntry(datastreamer.EntryType(entryType), em); err != nil {
				return err
			}
		}
	}
	return nil
}

func createBlockWithBatchCheckStreamEntriesProto(
	reader DbReader,
	tx kv.Tx,
	block,
	lastBlock *eritypes.Block,
	batchNumber,
	lastBatchNumber,
	chainId,
	forkId uint64,
	shouldSkipBatchEndEntry bool,
) (*DataStreamEntries, error) {
	var err error
	var endEntriesProto []DataStreamEntryProto
	var startEntriesProto, blockEntries *DataStreamEntries
	// we might have a series of empty batches to account for, so we need to know the gap
	batchGap := batchNumber - lastBatchNumber
	isBatchStart := batchGap > 0

	// batch start
	// BATCH BOOKMARK
	if isBatchStart {
		gers, err := reader.GetBatchGlobalExitRootsProto(lastBatchNumber, batchNumber)
		if err != nil {
			return nil, err
		}
		// the genesis we insert fully, so we would have to skip closing it
		if !shouldSkipBatchEndEntry {
			localExitRoot, err := utils.GetBatchLocalExitRootFromSCStorageForLatestBlock(batchNumber, reader, tx)
			if err != nil {
				return nil, err
			}
			lastBlockRoot := lastBlock.Root()
			if endEntriesProto, err = addBatchEndEntriesProto(lastBatchNumber, &lastBlockRoot, gers, &localExitRoot); err != nil {
				return nil, err
			}
		}

		if startEntriesProto, err = createBatchStartEntriesProto(reader, tx, batchNumber, lastBatchNumber, batchGap, chainId, block.Root(), gers); err != nil {
			return nil, err
		}
	}

	blockNum := block.NumberU64()

	l1InfoTreeMinTimestamps := make(map[uint64]uint64)
	deltaTimestamp := block.Time() - lastBlock.Time()
	if blockNum == 1 {
		deltaTimestamp = block.Time()
		l1InfoTreeMinTimestamps[0] = 0
	}

	if blockEntries, err = createFullBlockStreamEntriesProto(reader, tx, block, block.Transactions(), forkId, deltaTimestamp, batchNumber, l1InfoTreeMinTimestamps); err != nil {
		return nil, err
	}

	if blockEntries.Size() == 0 {
		return nil, fmt.Errorf("didn't create any entries for block %d", blockNum)
	}

	entries := NewDataStreamEntries(len(endEntriesProto) + startEntriesProto.Size() + blockEntries.Size())
	entries.AddMany(endEntriesProto)
	entries.AddMany(startEntriesProto.Entries())
	entries.AddMany(blockEntries.Entries())

	return entries, nil
}

func createFullBlockStreamEntriesProto(
	reader DbReader,
	tx kv.Tx,
	block *eritypes.Block,
	filteredTransactions eritypes.Transactions,
	forkId,
	deltaTimestamp,
	batchNumber uint64,
	l1InfoTreeMinTimestamps map[uint64]uint64,
) (*DataStreamEntries, error) {
	entries := NewDataStreamEntries(len(filteredTransactions) + 3) // block bookmark + block + block end
	blockNum := block.NumberU64()
	// L2 BLOCK BOOKMARK
	entries.Add(newL2BlockBookmarkEntryProto(blockNum))

	ger, err := reader.GetBlockGlobalExitRoot(blockNum)
	if err != nil {
		return nil, err
	}
	l1BlockHash, err := reader.GetBlockL1BlockHash(blockNum)
	if err != nil {
		return nil, err
	}

	l1InfoIndex, err := reader.GetBlockL1InfoTreeIndex(blockNum)
	if err != nil {
		return nil, err
	}

	if l1InfoIndex > 0 {
		// get the l1 info data, so we can add the min timestamp to the map
		l1Info, err := reader.GetL1InfoTreeUpdate(l1InfoIndex)
		if err != nil {
			return nil, err
		}
		if l1Info != nil {
			l1InfoTreeMinTimestamps[l1InfoIndex] = l1Info.Timestamp
		}
	}

	blockInfoRoot, err := reader.GetBlockInfoRoot(blockNum)
	if err != nil {
		return nil, err
	}

	// L2 BLOCK
	entries.Add(newL2BlockProto(block, block.Hash().Bytes(), batchNumber, ger, uint32(deltaTimestamp), uint32(l1InfoIndex), l1BlockHash, l1InfoTreeMinTimestamps[l1InfoIndex], blockInfoRoot))

	var transaction DataStreamEntryProto
	isEtrog := forkId <= EtrogBatchNumber
	for _, tx := range filteredTransactions {
		if transaction, err = createTransactionEntryProto(reader, tx, blockNum, isEtrog); err != nil {
			return nil, err
		}
		entries.Add(transaction)
	}

	entries.Add(newL2BlockEndProto(blockNum))

	return entries, nil
}

func createTransactionEntryProto(
	reader DbReader,
	tx eritypes.Transaction,
	blockNum uint64,
	isEtrog bool,
) (txProto DataStreamEntryProto, err error) {
	effectiveGasPricePercentage, err := reader.GetEffectiveGasPricePercentage(tx.Hash())
	if err != nil {
		return nil, err
	}

	var intermediateRoot libcommon.Hash
	if isEtrog {
		if intermediateRoot, err = reader.GetIntermediateTxStateRoot(blockNum, tx.Hash()); err != nil {
			return nil, err
		}
	}

	// TRANSACTION

	if txProto, err = newTransactionProto(effectiveGasPricePercentage, intermediateRoot, tx, blockNum); err != nil {
		return nil, err
	}

	return txProto, nil
}

func BuildWholeBatchStreamEntriesProto(
	tx kv.Tx,
	reader DbReader,
	chainId uint64,
	previousBatchNumber,
	batchNumber uint64,
	blocks []eritypes.Block,
	txsPerBlock map[uint64][]eritypes.Transaction,
	l1InfoTreeMinTimestamps map[uint64]uint64,
) (allEntries *DataStreamEntries, err error) {
	var batchEndEntries []DataStreamEntryProto
	var batchStartEntries *DataStreamEntries

	forkId, err := reader.GetForkId(batchNumber)
	if err != nil {
		return nil, err
	}

	gers, err := reader.GetBatchGlobalExitRootsProto(previousBatchNumber, batchNumber)
	if err != nil {
		return nil, err
	}

	if batchStartEntries, err = createBatchStartEntriesProto(reader, tx, batchNumber, previousBatchNumber, batchNumber-previousBatchNumber, chainId, blocks[0].Root(), gers); err != nil {
		return nil, err
	}

	prevBatchLastBlock, err := rawdb.ReadBlockByNumber(tx, blocks[0].NumberU64()-1)
	if err != nil {
		return nil, err
	}

	lastBlock := *prevBatchLastBlock

	blocksEntries := make([]DataStreamEntryProto, 0)

	for _, block := range blocks {
		blockNum := block.NumberU64()

		deltaTimestamp := block.Time() - lastBlock.Time()
		if blockNum == 1 {
			deltaTimestamp = block.Time()
			l1InfoTreeMinTimestamps[0] = 0
		}

		txForBlock, found := txsPerBlock[blockNum]
		if !found {
			return nil, fmt.Errorf("no transactions array found for block %d", blockNum)
		}

		blockEntries, err := createFullBlockStreamEntriesProto(reader, tx, &block, txForBlock, forkId, deltaTimestamp, batchNumber, l1InfoTreeMinTimestamps)
		if err != nil {
			return nil, err
		}
		blocksEntries = append(blocksEntries, blockEntries.Entries()...)

		lastBlock = block
	}

	// the genesis we insert fully, so we would have to skip closing it
	localExitRoot, err := utils.GetBatchLocalExitRootFromSCStorageForLatestBlock(batchNumber, reader, tx)
	if err != nil {
		return nil, err
	}

	blockRoot := lastBlock.Root()

	batchEndEntries, err = addBatchEndEntriesProto(batchNumber, &blockRoot, gers, &localExitRoot)
	if err != nil {
		return nil, err
	}

	allEntries = NewDataStreamEntries(batchStartEntries.Size() + len(blocksEntries) + len(batchEndEntries))
	allEntries.AddMany(batchStartEntries.Entries())
	allEntries.AddMany(blocksEntries)
	allEntries.AddMany(batchEndEntries)

	return allEntries, nil
}

func (srv *DataStreamServer) IsLastEntryBatchEnd() (isBatchEnd bool, err error) {
	header := srv.stream.GetHeader()

	if header.TotalEntries == 0 {
		return false, nil
	}

	//find end block entry to delete from it onward
	entryNum := header.TotalEntries - 1
	var entry datastreamer.FileEntry
	entry, err = srv.stream.GetEntry(entryNum)
	if err != nil {
		return false, err
	}

	return uint32(entry.Type) == uint32(types.EntryTypeBatchEnd), nil
}

func (srv *DataStreamServer) GetHighestBlockNumber() (uint64, error) {
	if srv.highestBlockWritten != nil {
		return *srv.highestBlockWritten, nil
	}

	header := srv.stream.GetHeader()

	if header.TotalEntries == 0 {
		return 0, nil
	}

	//find end block entry to delete from it onward
	entryNum := header.TotalEntries - 1
	var err error
	var entry datastreamer.FileEntry
	for {
		entry, err = srv.stream.GetEntry(entryNum)
		if err != nil {
			return 0, err
		}
		if uint32(entry.Type) == uint32(types.EntryTypeL2Block) || uint32(entry.Type) == uint32(types.EntryTypeL2Tx) {
			break
		}
		entryNum -= 1
	}

	if uint32(entry.Type) == uint32(types.EntryTypeL2Block) {
		l2Block, err := types.UnmarshalL2Block(entry.Data)
		if err != nil {
			return 0, err
		}

		return l2Block.L2BlockNumber, nil
	} else if uint32(entry.Type) == uint32(types.EntryTypeL2Tx) {
		tx, err := types.UnmarshalTx(entry.Data)
		if err != nil {
			return 0, err
		}

		return tx.L2BlockNumber, nil
	}

	return 0, nil
}

func (srv *DataStreamServer) GetHighestBatchNumber() (uint64, error) {
	if srv.highestBatchWritten != nil {
		return *srv.highestBatchWritten, nil
	}

	entry, found, err := srv.getLastEntryOfType(datastreamer.EntryType(types.EntryTypeBatchStart))
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}

	batch, err := types.UnmarshalBatchStart(entry.Data)
	if err != nil {
		return 0, err
	}

	srv.highestBatchWritten = &batch.Number

	return batch.Number, nil
}

func (srv *DataStreamServer) GetHighestClosedBatch() (uint64, error) {
	if srv.highestClosedBatchWritten != nil {
		return *srv.highestClosedBatchWritten, nil
	}
	entry, found, err := srv.getLastEntryOfType(datastreamer.EntryType(types.EntryTypeBatchEnd))
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}

	batch, err := types.UnmarshalBatchEnd(entry.Data)
	if err != nil {
		return 0, err
	}

	srv.highestClosedBatchWritten = &batch.Number

	return batch.Number, nil
}

// must be done on offline server
// finds the position of the block bookmark entry and deletes from it onward
// blockNumber 10 would return the stream to before block 10 bookmark
func (srv *DataStreamServer) UnwindToBlock(blockNumber uint64) error {
	// check if server is online

	// find blockend entry
	bookmark := types.NewBookmarkProto(blockNumber, datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK)
	marshalled, err := bookmark.Marshal()
	if err != nil {
		return err
	}
	entryNum, err := srv.stream.GetBookmark(marshalled)
	if err != nil {
		return err
	}

	return srv.stream.TruncateFile(entryNum)
}

// must be done on offline server
// finds the position of the endBlock entry for the given number
// and unwinds the datastream file to it
func (srv *DataStreamServer) UnwindToBatchStart(batchNumber uint64) error {
	// check if server is online

	// find blockend entry
	bookmark := types.NewBookmarkProto(batchNumber, datastream.BookmarkType_BOOKMARK_TYPE_BATCH)
	marshalled, err := bookmark.Marshal()
	if err != nil {
		return err
	}
	entryNum, err := srv.stream.GetBookmark(marshalled)
	if err != nil {
		return err
	}

	return srv.stream.TruncateFile(entryNum)
}

func (srv *DataStreamServer) getLastEntryOfType(entryType datastreamer.EntryType) (datastreamer.FileEntry, bool, error) {
	header := srv.stream.GetHeader()
	emtryEntry := datastreamer.FileEntry{}

	// loop will become infinite if using unsigned type
	for entryNum := int64(header.TotalEntries - 1); entryNum >= 0; entryNum-- {
		entry, err := srv.stream.GetEntry(uint64(entryNum))
		if err != nil {
			return emtryEntry, false, err
		}
		if entry.Type == entryType {
			return entry, true, nil
		}
	}

	return emtryEntry, false, nil
}
