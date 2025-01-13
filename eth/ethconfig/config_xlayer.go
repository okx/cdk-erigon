package ethconfig

import (
	"time"
)

// XLayerConfig is the X Layer config used on the eth backend
type XLayerConfig struct {
	Apollo                ApolloClientConfig
	Nacos                 NacosConfig
	EnableInnerTx         bool
	EnableInnerTxByTracer bool
	// Sequencer
	SequencerBatchSleepDuration time.Duration
}

var DefaultXLayerConfig = XLayerConfig{}

// NacosConfig is the config for nacos
type NacosConfig struct {
	URLs               string
	NamespaceId        string
	ApplicationName    string
	ExternalListenAddr string
}

// ApolloClientConfig is the config for apollo
type ApolloClientConfig struct {
	Enable        bool
	IP            string
	AppID         string
	NamespaceName string
}
