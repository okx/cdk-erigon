package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/remotedb"
	"github.com/ledgerwatch/erigon-lib/kv/remotedbserver"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"
	"github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	common2 "github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/ledgerwatch/erigon/zk/txpool/txpooluitl"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
)

var (
	sentryAddr     []string // Address of the sentry <host>:<port>
	traceSenders   []string
	privateApiAddr string
	txpoolApiAddr  string
	datadirCli     string // Path to td working dir

	TLSCertfile string
	TLSCACert   string
	TLSKeyFile  string

	pendingPoolLimit int
	baseFeePoolLimit int
	queuedPoolLimit  int

	priceLimit         uint64
	accountSlots       uint64
	blobSlots          uint64
	totalBlobPoolLimit uint64
	priceBump          uint64
	blobPriceBump      uint64

	noTxGossip bool

	// For X Layer
	enableWhiteList      bool
	whiteList            []string
	blockList            []string
	freeClaimGasAddrs    []string
	gasPriceMultiple     uint64
	enableFreeGasByNonce bool
	freeGasExAddrs       []string
	freeGasCountPerAddr  uint64
	freeGasLimit         uint64
	enableFreeGasList    bool
	freeGasList          string

	commitEvery   time.Duration
	purgeEvery    time.Duration
	purgeDistance time.Duration
)

func init() {
	utils.CobraFlags(rootCmd, debug.Flags, utils.MetricFlags, logging.Flags)
	rootCmd.Flags().StringSliceVar(&sentryAddr, "sentry.api.addr", []string{"localhost:9091"}, "comma separated sentry addresses '<host>:<port>,<host>:<port>'")
	rootCmd.Flags().StringVar(&privateApiAddr, "private.api.addr", "localhost:9090", "execution service <host>:<port>")
	rootCmd.Flags().StringVar(&txpoolApiAddr, "txpool.api.addr", "localhost:9094", "txpool service <host>:<port>")
	rootCmd.Flags().StringVar(&datadirCli, utils.DataDirFlag.Name, paths.DefaultDataDir(), utils.DataDirFlag.Usage)
	if err := rootCmd.MarkFlagDirname(utils.DataDirFlag.Name); err != nil {
		panic(err)
	}
	rootCmd.PersistentFlags().StringVar(&TLSCertfile, "tls.cert", "", "certificate for client side TLS handshake")
	rootCmd.PersistentFlags().StringVar(&TLSKeyFile, "tls.key", "", "key file for client side TLS handshake")
	rootCmd.PersistentFlags().StringVar(&TLSCACert, "tls.cacert", "", "CA certificate for client side TLS handshake")

	rootCmd.PersistentFlags().IntVar(&pendingPoolLimit, "txpool.globalslots", txpoolcfg.DefaultConfig.PendingSubPoolLimit, "Maximum number of executable transaction slots for all accounts")
	rootCmd.PersistentFlags().IntVar(&baseFeePoolLimit, "txpool.globalbasefeeslots", txpoolcfg.DefaultConfig.BaseFeeSubPoolLimit, "Maximum number of non-executable transactions where only not enough baseFee")
	rootCmd.PersistentFlags().IntVar(&queuedPoolLimit, "txpool.globalqueue", txpoolcfg.DefaultConfig.QueuedSubPoolLimit, "Maximum number of non-executable transaction slots for all accounts")
	rootCmd.PersistentFlags().Uint64Var(&priceLimit, "txpool.pricelimit", txpoolcfg.DefaultConfig.MinFeeCap, "Minimum gas price (fee cap) limit to enforce for acceptance into the pool")
	rootCmd.PersistentFlags().Uint64Var(&accountSlots, "txpool.accountslots", txpoolcfg.DefaultConfig.AccountSlots, "Minimum number of executable transaction slots guaranteed per account")
	rootCmd.PersistentFlags().Uint64Var(&blobSlots, "txpool.blobslots", txpoolcfg.DefaultConfig.BlobSlots, "Max allowed total number of blobs (within type-3 txs) per account")
	rootCmd.PersistentFlags().Uint64Var(&totalBlobPoolLimit, "txpool.totalblobpoollimit", txpoolcfg.DefaultConfig.TotalBlobPoolLimit, "Total limit of number of all blobs in txs within the txpool")
	rootCmd.PersistentFlags().Uint64Var(&priceBump, "txpool.pricebump", txpoolcfg.DefaultConfig.PriceBump, "Price bump percentage to replace an already existing transaction")
	rootCmd.PersistentFlags().Uint64Var(&blobPriceBump, "txpool.blobpricebump", txpoolcfg.DefaultConfig.BlobPriceBump, "Price bump percentage to replace an existing blob (type-3) transaction")
	rootCmd.PersistentFlags().DurationVar(&commitEvery, utils.TxPoolCommitEveryFlag.Name, utils.TxPoolCommitEveryFlag.Value, utils.TxPoolCommitEveryFlag.Usage)
	rootCmd.PersistentFlags().DurationVar(&purgeEvery, utils.TxpoolPurgeEveryFlag.Name, utils.TxpoolPurgeEveryFlag.Value, utils.TxpoolPurgeEveryFlag.Usage)
	rootCmd.PersistentFlags().DurationVar(&purgeDistance, utils.TxpoolPurgeDistanceFlag.Name, utils.TxpoolPurgeDistanceFlag.Value, utils.TxpoolPurgeDistanceFlag.Usage)
	rootCmd.PersistentFlags().BoolVar(&noTxGossip, utils.TxPoolGossipDisableFlag.Name, utils.TxPoolGossipDisableFlag.Value, utils.TxPoolGossipDisableFlag.Usage)
	rootCmd.Flags().StringSliceVar(&traceSenders, utils.TxPoolTraceSendersFlag.Name, []string{}, utils.TxPoolTraceSendersFlag.Usage)
	// For X Layer
	rootCmd.Flags().StringSliceVar(&freeClaimGasAddrs, utils.TxPoolPackBatchSpecialList.Name, ethconfig.DeprecatedDefaultTxPoolConfig.FreeClaimGasAddrs, utils.TxPoolPackBatchSpecialList.Usage)
	rootCmd.Flags().Uint64Var(&gasPriceMultiple, utils.TxPoolGasPriceMultiple.Name, ethconfig.DeprecatedDefaultTxPoolConfig.GasPriceMultiple, utils.TxPoolGasPriceMultiple.Usage)
	rootCmd.Flags().BoolVar(&enableWhiteList, utils.TxPoolEnableWhitelistFlag.Name, ethconfig.DeprecatedDefaultTxPoolConfig.EnableWhitelist, utils.TxPoolEnableWhitelistFlag.Usage)
	rootCmd.Flags().StringSliceVar(&whiteList, utils.TxPoolWhiteList.Name, ethconfig.DeprecatedDefaultTxPoolConfig.WhiteList, utils.TxPoolWhiteList.Usage)
	rootCmd.Flags().StringSliceVar(&blockList, utils.TxPoolBlockedList.Name, ethconfig.DeprecatedDefaultTxPoolConfig.BlockedList, utils.TxPoolBlockedList.Usage)
	rootCmd.Flags().BoolVar(&enableFreeGasByNonce, utils.TxPoolEnableFreeGasByNonce.Name, ethconfig.DeprecatedDefaultTxPoolConfig.EnableFreeGasByNonce, utils.TxPoolEnableFreeGasByNonce.Usage)
	rootCmd.Flags().StringSliceVar(&freeGasExAddrs, utils.TxPoolFreeGasExAddrs.Name, ethconfig.DeprecatedDefaultTxPoolConfig.FreeGasExAddrs, utils.TxPoolFreeGasExAddrs.Usage)
	rootCmd.PersistentFlags().Uint64Var(&freeGasCountPerAddr, utils.TxPoolFreeGasCountPerAddr.Name, ethconfig.DeprecatedDefaultTxPoolConfig.FreeGasCountPerAddr, utils.TxPoolFreeGasCountPerAddr.Usage)
	rootCmd.PersistentFlags().Uint64Var(&freeGasLimit, utils.TxPoolFreeGasLimit.Name, ethconfig.DeprecatedDefaultTxPoolConfig.FreeGasLimit, utils.TxPoolFreeGasLimit.Usage)
	rootCmd.Flags().BoolVar(&enableFreeGasList, utils.TxPoolEnableFreeGasList.Name, ethconfig.DeprecatedDefaultTxPoolConfig.EnableFreeGasList, utils.TxPoolEnableFreeGasList.Usage)
	rootCmd.PersistentFlags().StringVar(&freeGasList, utils.TxPoolFreeGasList.Name, "", utils.TxPoolFreeGasList.Usage)
}

var rootCmd = &cobra.Command{
	Use:   "txpool",
	Short: "Launch external Transaction Pool instance - same as built-into Erigon, but as independent Process",
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		if err := doTxpool(cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

func doTxpool(ctx context.Context, logger log.Logger) error {
	creds, err := grpcutil.TLS(TLSCACert, TLSCertfile, TLSKeyFile)
	if err != nil {
		return fmt.Errorf("could not connect to remoteKv: %w", err)
	}
	coreConn, err := grpcutil.Connect(creds, privateApiAddr)
	if err != nil {
		return fmt.Errorf("could not connect to remoteKv: %w", err)
	}

	kvClient := remote.NewKVClient(coreConn)
	coreDB, err := remotedb.NewRemote(gointerfaces.VersionFromProto(remotedbserver.KvServiceAPIVersion), log.New(), kvClient).Open()
	if err != nil {
		return fmt.Errorf("could not connect to remoteKv: %w", err)
	}

	log.Info("TxPool started", "db", filepath.Join(datadirCli, "txpool"))

	sentryClients := make([]direct.SentryClient, len(sentryAddr))
	for i := range sentryAddr {
		creds, err := grpcutil.TLS(TLSCACert, TLSCertfile, TLSKeyFile)
		if err != nil {
			return fmt.Errorf("could not connect to sentry: %w", err)
		}
		sentryConn, err := grpcutil.Connect(creds, sentryAddr[i])
		if err != nil {
			return fmt.Errorf("could not connect to sentry: %w", err)
		}

		sentryClients[i] = direct.NewSentryClientRemote(proto_sentry.NewSentryClient(sentryConn))
	}

	cfg := txpoolcfg.DefaultConfig
	dirs := datadir.New(datadirCli)

	cfg.DBDir = dirs.TxPool

	cfg.CommitEvery = common2.RandomizeDuration(commitEvery)
	cfg.PurgeEvery = common2.RandomizeDuration(purgeEvery)
	cfg.PurgeDistance = purgeDistance
	cfg.PendingSubPoolLimit = pendingPoolLimit
	cfg.BaseFeeSubPoolLimit = baseFeePoolLimit
	cfg.QueuedSubPoolLimit = queuedPoolLimit
	cfg.MinFeeCap = priceLimit
	cfg.AccountSlots = accountSlots
	cfg.BlobSlots = blobSlots
	cfg.TotalBlobPoolLimit = totalBlobPoolLimit
	cfg.PriceBump = priceBump
	cfg.BlobPriceBump = blobPriceBump
	cfg.NoGossip = noTxGossip

	cacheConfig := kvcache.DefaultCoherentConfig
	cacheConfig.MetricsLabel = "txpool"

	cfg.TracedSenders = make([]string, len(traceSenders))
	for i, senderHex := range traceSenders {
		sender := common.HexToAddress(senderHex)
		cfg.TracedSenders[i] = string(sender[:])
	}

	// For X Layer tx pool access
	ethCfg := &ethconfig.Defaults
	ethCfg.DeprecatedTxPool.EnableWhitelist = enableWhiteList
	ethCfg.DeprecatedTxPool.WhiteList = make([]string, len(whiteList))
	for i, addrHex := range whiteList {
		addr := common.HexToAddress(addrHex)
		ethCfg.DeprecatedTxPool.WhiteList[i] = addr.String()
	}
	ethCfg.DeprecatedTxPool.BlockedList = make([]string, len(blockList))
	for i, addrHex := range blockList {
		addr := common.HexToAddress(addrHex)
		ethCfg.DeprecatedTxPool.BlockedList[i] = addr.String()
	}
	ethCfg.DeprecatedTxPool.FreeClaimGasAddrs = make([]string, len(freeClaimGasAddrs))
	for i, addrHex := range freeClaimGasAddrs {
		addr := common.HexToAddress(addrHex)
		ethCfg.DeprecatedTxPool.FreeClaimGasAddrs[i] = addr.String()
	}
	ethCfg.DeprecatedTxPool.GasPriceMultiple = gasPriceMultiple
	ethCfg.DeprecatedTxPool.EnableFreeGasByNonce = enableFreeGasByNonce
	ethCfg.DeprecatedTxPool.FreeGasExAddrs = make([]string, len(freeGasExAddrs))
	for i, addrHex := range freeGasExAddrs {
		addr := common.HexToAddress(addrHex)
		ethCfg.DeprecatedTxPool.FreeGasExAddrs[i] = addr.String()
	}
	ethCfg.DeprecatedTxPool.FreeGasCountPerAddr = freeGasCountPerAddr
	ethCfg.DeprecatedTxPool.FreeGasLimit = freeGasLimit
	ethCfg.DeprecatedTxPool.EnableFreeGasList = enableFreeGasList
	if len(freeGasList) > 0 {
		if err := jsoniter.UnmarshalFromString(freeGasList, &ethCfg.DeprecatedTxPool.FreeGasList); err != nil {
			panic("unable to unmarshal freeGasList:" + err.Error())
		}
	}

	newTxs := make(chan types.Announcements, 1024)
	defer close(newTxs)
	txPoolDB, txPool, fetch, send, txpoolGrpcServer, err := txpooluitl.AllComponents(ctx, cfg, ethCfg,
		kvcache.New(cacheConfig), newTxs, coreDB, sentryClients, kvClient)
	if err != nil {
		return err
	}
	fetch.ConnectCore()
	fetch.ConnectSentries()

	miningGrpcServer := privateapi.NewMiningServer(ctx, &rpcdaemontest.IsMiningMock{}, nil, logger)

	grpcServer, err := txpool.StartGrpc(txpoolGrpcServer, miningGrpcServer, txpoolApiAddr, nil)
	if err != nil {
		return err
	}

	notifyMiner := func() {}
	txpool.MainLoop(ctx, txPoolDB, coreDB, txPool, newTxs, send, txpoolGrpcServer.NewSlotsStreams, notifyMiner)

	grpcServer.GracefulStop()
	return nil
}

func main() {
	ctx, cancel := common.RootContext()
	defer cancel()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
