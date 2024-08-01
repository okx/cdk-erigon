package operations

import (
	"context"
	"math/big"
	"strings"
	"time"

	erigonchain "github.com/gateway-fm/cdk-erigon-lib/chain"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/ethclient"
	"github.com/ledgerwatch/erigon/zkevm/log"
	"google.golang.org/grpc/balancer/grpclb/state"
)

// Public shared
const (
	DefaultSequencerAddress           = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
	DefaultSequencerPrivateKey        = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
	DefaultL1NetworkURL               = "http://localhost:8545"
	DefaultL1ChainID           uint64 = 1337

	DefaultL2NetworkURL        = "http://localhost:8124"
	DefaultL2ChainID    uint64 = 195

	DefaultTimeoutTxToBeMined = 1 * time.Minute

	DefaultL2AdminAddress    = "0x8f8E2d6cF621f30e9a11309D6A56A876281Fd534"
	DefaultL2AdminPrivateKey = "0x815405dddb0e2a99b12af775fd2929e526704e1d1aea6a0b4e74dc33e2f7fcd2"
)

// Manager controls operations and has knowledge about how to set up and tear
// down a functional environment.
type Manager struct {
	ctx context.Context

	st   *state.State
	wait *Wait
}

// ApplyL1Txs sends the given L1 txs, waits for them to be consolidated and checks the final state.
func ApplyL1Txs(ctx context.Context, txs []*types.Transaction, auth *bind.TransactOpts, client *ethclient.Client) error {
	_, err := applyTxs(ctx, txs, auth, client, true)
	return err
}

// ConfirmationLevel type used to describe the confirmation level of a transaction
type ConfirmationLevel int

// PoolConfirmationLevel indicates that transaction is added into the pool
const PoolConfirmationLevel ConfirmationLevel = 0

// TrustedConfirmationLevel indicates that transaction is  added into the trusted state
const TrustedConfirmationLevel ConfirmationLevel = 1

// VirtualConfirmationLevel indicates that transaction is  added into the virtual state
const VirtualConfirmationLevel ConfirmationLevel = 2

// VerifiedConfirmationLevel indicates that transaction is  added into the verified state
const VerifiedConfirmationLevel ConfirmationLevel = 3

// ApplyL2Txs sends the given L2 txs, waits for them to be consolidated and
// checks the final state.
func ApplyL2Txs(ctx context.Context, txs []*types.Transaction, auth *bind.TransactOpts, client *ethclient.Client, confirmationLevel ConfirmationLevel) ([]*big.Int, error) {
	var err error
	if auth == nil {
		auth, err = GetAuth(DefaultSequencerPrivateKey, DefaultL2ChainID)
		if err != nil {
			return nil, err
		}
	}

	if client == nil {
		client, err = ethclient.Dial(DefaultL2NetworkURL)
		if err != nil {
			return nil, err
		}
	}
	waitToBeMined := confirmationLevel != PoolConfirmationLevel
	sentTxs, err := applyTxs(ctx, txs, auth, client, waitToBeMined)
	if err != nil {
		return nil, err
	}
	if confirmationLevel == PoolConfirmationLevel {
		return nil, nil
	}

	l2BlockNumbers := make([]*big.Int, 0, len(sentTxs))
	for _, txTemp := range sentTxs {
		// check transaction nonce against transaction reported L2 block number
		receipt, err := client.TransactionReceipt(ctx, txTemp.Hash())
		if err != nil {
			return nil, err
		}

		// get L2 block number
		l2BlockNumbers = append(l2BlockNumbers, receipt.BlockNumber)
		if confirmationLevel == TrustedConfirmationLevel {
			continue
		}

		// wait for l2 block to be virtualized
		log.Infof("waiting for the block number %v to be virtualized", receipt.BlockNumber.String())
		err = WaitL2BlockToBeVirtualized(receipt.BlockNumber, 4*time.Minute) //nolint:gomnd
		if err != nil {
			return nil, err
		}
		if confirmationLevel == VirtualConfirmationLevel {
			continue
		}

		// wait for l2 block number to be consolidated
		log.Infof("waiting for the block number %v to be consolidated", receipt.BlockNumber.String())
		err = WaitL2BlockToBeConsolidated(receipt.BlockNumber, 4*time.Minute) //nolint:gomnd
		if err != nil {
			return nil, err
		}
	}

	return l2BlockNumbers, nil
}

func applyTxs(ctx context.Context, txs []*types.Transaction, auth *bind.TransactOpts, client *ethclient.Client, waitToBeMined bool) ([]types.Transaction, error) {
	var sentTxs []types.Transaction

	for i := 0; i < len(txs); i++ {
		signedTx, err := auth.Signer(auth.From, *txs[i])
		if err != nil {
			return nil, err
		}
		log.Infof("Sending Tx %v Nonce %v", signedTx.Hash(), signedTx.GetNonce())
		err = client.SendTransaction(context.Background(), signedTx)
		if err != nil {
			return nil, err
		}

		sentTxs = append(sentTxs, signedTx)
	}
	if !waitToBeMined {
		return nil, nil
	}

	// wait for TX to be mined
	timeout := 180 * time.Second //nolint:gomnd
	for _, tx := range sentTxs {
		log.Infof("Waiting Tx %s to be mined", tx.Hash())
		err := WaitTxToBeMined(ctx, client, tx, timeout)
		if err != nil {
			return nil, err
		}
		log.Infof("Tx %s mined successfully", tx.Hash())
	}
	nTxs := len(txs)
	if nTxs > 1 {
		log.Infof("%d transactions added into the trusted state successfully.", nTxs)
	} else {
		log.Info("transaction added into the trusted state successfully.")
	}

	return sentTxs, nil
}

// GetAuth configures and returns an auth object.
func GetAuth(privateKeyStr string, chainID uint64) (*bind.TransactOpts, error) {
	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(privateKeyStr, "0x"))
	if err != nil {
		return nil, err
	}

	return bind.NewKeyedTransactorWithChainID(privateKey, big.NewInt(0).SetUint64(chainID))
}

// MustGetAuth GetAuth but panics if err
func MustGetAuth(privateKeyStr string, chainID uint64) *bind.TransactOpts {
	auth, err := GetAuth(privateKeyStr, chainID)
	if err != nil {
		panic(err)
	}
	return auth
}

// GetClient returns an ethereum client to the provided URL
func GetClient(URL string) (*ethclient.Client, error) {
	client, err := ethclient.Dial(URL)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func GetTestChainConfig(chainID uint64) *chain.Config {
	return &chain.Config{
		ChainID:               big.NewInt(int64(chainID)),
		Consensus:             erigonchain.EtHashConsensus,
		HomesteadBlock:        big.NewInt(0),
		TangerineWhistleBlock: big.NewInt(0),
		SpuriousDragonBlock:   big.NewInt(0),
		ByzantiumBlock:        big.NewInt(0),
		ConstantinopleBlock:   big.NewInt(0),
		PetersburgBlock:       big.NewInt(0),
		IstanbulBlock:         big.NewInt(0),
		MuirGlacierBlock:      big.NewInt(0),
		BerlinBlock:           big.NewInt(0),
		Ethash:                new(erigonchain.EthashConfig),
	}
}
