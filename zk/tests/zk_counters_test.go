package vm

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus/ethash/ethashcfg"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconsensusconfig"
	"github.com/ledgerwatch/erigon/params"
	seq "github.com/ledgerwatch/erigon/zk/sequencer"
	"github.com/ledgerwatch/erigon/zk/tx"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/erigon/zkevm/hex"
	"github.com/status-im/keycard-go/hexutils"
)

const root = "./testdata"
const transactionGasLimit = 30000000

var (
	noop = state.NewNoopWriter()
)

type vector struct {
	BatchL2Data        string `json:"batchL2Data"`
	BatchL2DataDecoded []byte
	Genesis            []struct {
		Address  string                      `json:"address"`
		Nonce    string                      `json:"nonce"`
		Balance  string                      `json:"balance"`
		PvtKey   string                      `json:"pvtKey"`
		ByteCode string                      `json:"bytecode"`
		Storage  map[common.Hash]common.Hash `json:"storage,omitempty"`
	} `json:"genesis"`
	VirtualCounters struct {
		Steps    int `json:"steps"`
		Arith    int `json:"arith"`
		Binary   int `json:"binary"`
		MemAlign int `json:"memAlign"`
		Keccaks  int `json:"keccaks"`
		Padding  int `json:"padding"`
		Poseidon int `json:"poseidon"`
		Sha256   int `json:"sha256"`
	} `json:"virtualCounters"`
	SequencerAddress string `json:"sequencerAddress"`
	ChainId          int64  `json:"chainID"`
	ForkId           uint64 `json:"forkID"`
	ExpectedOldRoot  string `json:"expectedOldRoot"`
	ExpectedNewRoot  string `json:"expectedNewRoot"`
	SmtDepths        []int  `json:"smtDepths"`
	Txs              [2]struct {
		Type           int    `json:"type"`
		DeltaTimestamp string `json:"deltaTimestamp"`
		L1Info         *struct {
			GlobalExitRoot string `json:"globalExitRoot"`
			BlockHash      string `json:"blockHash"`
			Timestamp      string `json:"timestamp"`
		} `json:"l1Info"`
	} `json:"txs"`
}

func Test_RunTestVectors(t *testing.T) {
	// we need to ensure we're running in a sequencer context to wrap the jump table
	os.Setenv(seq.SEQUENCER_ENV_KEY, "1")
	defer os.Setenv(seq.SEQUENCER_ENV_KEY, "0")

	files, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}

	var tests []vector
	var fileNames []string

	for _, file := range files {
		var inner []vector
		contents, err := os.ReadFile(fmt.Sprintf("%s/%s", root, file.Name()))
		if err != nil {
			t.Fatal(err)
		}

		if err = json.Unmarshal(contents, &inner); err != nil {
			t.Fatal(err)
		}
		for i := len(inner) - 1; i >= 0; i-- {
			fileNames = append(fileNames, file.Name())
		}
		tests = append(tests, inner...)
	}

	for idx, test := range tests {
		t.Run(fileNames[idx], func(t *testing.T) {
			runTest(t, test, err, fileNames[idx], idx)
		})
	}
}

func runTest(t *testing.T, test vector, err error, fileName string, idx int) {
	test.BatchL2DataDecoded, err = hex.DecodeHex(test.BatchL2Data)
	if err != nil {
		t.Fatal(err)
	}

	decodedBlocks, err := tx.DecodeBatchL2Blocks(test.BatchL2DataDecoded, test.ForkId)
	if err != nil {
		t.Fatal(err)
	}
	if len(decodedBlocks) == 0 {
		fmt.Printf("found no blocks in file %s", fileName)
	}
	for _, block := range decodedBlocks {
		if len(block.Transactions) == 0 {
			fmt.Printf("found no transactions in file %s", fileName)
		}
	}

	db, tx := memdb.NewTestTx(t)
	defer db.Close()
	defer tx.Rollback()

	for _, table := range kv.ChaindataTables {
		if err = tx.CreateBucket(table); err != nil {
			t.Fatal(err)
		}
	}

	genesisAccounts := map[common.Address]types.GenesisAccount{}

	for _, g := range test.Genesis {
		addr := common.HexToAddress(g.Address)
		key, err := hex.DecodeHex(g.PvtKey)
		if err != nil {
			t.Fatal(err)
		}
		nonce, err := strconv.ParseUint(g.Nonce, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		balance, ok := new(big.Int).SetString(g.Balance, 10)
		if !ok {
			t.Fatal(errors.New("could not parse balance"))
		}
		code, err := hex.DecodeHex(g.ByteCode)
		if err != nil {
			t.Fatal(err)
		}
		acc := types.GenesisAccount{
			Balance:    balance,
			Nonce:      nonce,
			PrivateKey: key,
			Code:       code,
			Storage:    g.Storage,
		}
		genesisAccounts[addr] = acc
	}

	genesis := &types.Genesis{
		Alloc: genesisAccounts,
		Config: &chain.Config{
			ChainID: big.NewInt(test.ChainId),
		},
	}

	genesisBlock, _, sparseTree, err := core.WriteGenesisState(genesis, tx, fmt.Sprintf("%s/temp-%v", os.TempDir(), idx))
	if err != nil {
		t.Fatal(err)
	}
	smtDepth := sparseTree.GetDepth()
	for len(test.SmtDepths) < len(decodedBlocks) {
		test.SmtDepths = append(test.SmtDepths, smtDepth)
	}
	if len(test.SmtDepths) == 0 {
		test.SmtDepths = append(test.SmtDepths, smtDepth)
	}

	genesisRoot := genesisBlock.Root()
	expectedGenesisRoot := common.HexToHash(test.ExpectedOldRoot)
	if genesisRoot != expectedGenesisRoot {
		t.Fatal("genesis root did not match expected")
	}

	sequencer := common.HexToAddress(test.SequencerAddress)

	header := &types.Header{
		Number:     big.NewInt(1),
		Difficulty: big.NewInt(0),
	}
	getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(tx, hash, number) }

	chainConfig := params.ChainConfigByChainName("hermez-dev")
	chainConfig.ChainID = big.NewInt(test.ChainId)

	ethashCfg := &ethashcfg.Config{
		CachesInMem:      1,
		CachesLockMmap:   true,
		DatasetDir:       "./dataset",
		DatasetsInMem:    1,
		DatasetsOnDisk:   1,
		DatasetsLockMmap: true,
		PowMode:          ethashcfg.ModeFake,
		NotifyFull:       false,
		Log:              nil,
	}

	engine := ethconsensusconfig.CreateConsensusEngine(chainConfig, ethashCfg, []string{}, true, "", "", true, "./datadir", nil, false /* readonly */, db)

	vmCfg := vm.ZkConfig{
		Config: vm.Config{
			Debug:         false,
			Tracer:        nil,
			NoRecursion:   false,
			NoBaseFee:     false,
			SkipAnalysis:  false,
			TraceJumpDest: false,
			NoReceipts:    false,
			ReadOnly:      false,
			StatelessExec: false,
			RestoreState:  false,
			ExtraEips:     nil,
		},
	}

	stateReader := state.NewPlainStateReader(tx)
	ibs := state.New(stateReader)

	if test.Txs[0].Type == 11 {
		parentRoot := common.Hash{}
		deltaTimestamp, _ := strconv.ParseUint(test.Txs[0].DeltaTimestamp, 10, 64)
		ibs.PreExecuteStateSet(chainConfig, 1, deltaTimestamp, &parentRoot)

		// handle writing to the ger manager contract
		if test.Txs[0].L1Info != nil {
			timestamp, _ := strconv.ParseUint(test.Txs[0].L1Info.Timestamp, 10, 64)
			ger := string(test.Txs[0].L1Info.GlobalExitRoot)
			blockHash := string(test.Txs[0].L1Info.BlockHash)

			hexutil.Remove0xPrefixIfExists(&ger)
			hexutil.Remove0xPrefixIfExists(&blockHash)

			l1info := &zktypes.L1InfoTreeUpdate{
				GER:        common.BytesToHash(hexutils.HexToBytes(ger)),
				ParentHash: common.BytesToHash(hexutils.HexToBytes(blockHash)),
				Timestamp:  timestamp,
			}
			// first check if this ger has already been written
			l1BlockHash := ibs.ReadGerManagerL1BlockHash(l1info.GER)
			if l1BlockHash == (common.Hash{}) {
				// not in the contract so let's write it!
				ibs.WriteGerManagerL1BlockHash(l1info.GER, l1info.ParentHash)
			}
		}
	}

	batchCollector := vm.NewBatchCounterCollector(test.SmtDepths[0], uint16(test.ForkId))

	blockStarted := false
	for i, block := range decodedBlocks {
		for _, transaction := range block.Transactions {
			if !blockStarted {
				overflow, err := batchCollector.StartNewBlock()
				if err != nil {
					t.Fatal(err)
				}
				if overflow {
					t.Fatal("unexpected overflow")
				}
				blockStarted = true
			}
			txCounters := vm.NewTransactionCounter(transaction, test.SmtDepths[i], false)
			overflow, err := batchCollector.AddNewTransactionCounters(txCounters)
			if err != nil {
				t.Fatal(err)
			}

			gasPool := new(core.GasPool).AddGas(transactionGasLimit)

			vmCfg.CounterCollector = txCounters.ExecutionCounters()

			_, result, err := core.ApplyTransaction_zkevm(
				chainConfig,
				core.GetHashFn(header, getHeader),
				engine,
				&sequencer,
				gasPool,
				ibs,
				noop,
				header,
				transaction,
				&header.GasUsed,
				vmCfg,
				big.NewInt(0), // parent excess data gas
				zktypes.EFFECTIVE_GAS_PRICE_PERCENTAGE_MAXIMUM)

			if err != nil {
				// this could be deliberate in the test so just move on and note it
				fmt.Println("err handling tx", err)
				continue
			}
			if overflow {
				t.Fatal("unexpected overflow")
			}

			if err = txCounters.ProcessTx(ibs, result.ReturnData); err != nil {
				t.Fatal(err)
			}
		}
	}

	combined, err := batchCollector.CombineCollectors()
	if err != nil {
		t.Fatal(err)
	}

	vc := test.VirtualCounters

	var errors []string
	if vc.Keccaks != combined[vm.K].Used() {
		errors = append(errors, fmt.Sprintf("K=%v:%v", combined[vm.K].Used(), vc.Keccaks))
	}
	if vc.Arith != combined[vm.A].Used() {
		errors = append(errors, fmt.Sprintf("A=%v:%v", combined[vm.A].Used(), vc.Arith))
	}
	if vc.Binary != combined[vm.B].Used() {
		errors = append(errors, fmt.Sprintf("B=%v:%v", combined[vm.B].Used(), vc.Binary))
	}
	if vc.Padding != combined[vm.D].Used() {
		errors = append(errors, fmt.Sprintf("D=%v:%v", combined[vm.D].Used(), vc.Padding))
	}
	if vc.Sha256 != combined[vm.SHA].Used() {
		errors = append(errors, fmt.Sprintf("SHA=%v:%v", combined[vm.SHA].Used(), vc.Sha256))
	}
	if vc.MemAlign != combined[vm.M].Used() {
		errors = append(errors, fmt.Sprintf("M=%v:%v", combined[vm.M].Used(), vc.MemAlign))
	}
	if vc.Poseidon != combined[vm.P].Used() {
		errors = append(errors, fmt.Sprintf("P=%v:%v", combined[vm.P].Used(), vc.Poseidon))
	}
	if vc.Steps != combined[vm.S].Used() {
		errors = append(errors, fmt.Sprintf("S=%v:%v", combined[vm.S].Used(), vc.Steps))
	}
	if len(errors) > 0 {
		t.Errorf("counter mismath in file %s: %s \n", fileName, strings.Join(errors, " "))
	}
}
