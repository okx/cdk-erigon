package main

import (
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/zk/apollo"
	"github.com/ledgerwatch/erigon/zk/metrics"
	"github.com/ledgerwatch/log/v3"
)

func initRunForXLayer(ethCfg *ethconfig.Config) {
	apolloClient := apollo.NewClient(ethCfg)
	if apolloClient.LoadConfig() {
		log.Info("Apollo config loaded")
	}

	// Init metrics
	if ethCfg.Zk.XLayer.Metrics.Enabled {
		metrics.Init()
	}
}
