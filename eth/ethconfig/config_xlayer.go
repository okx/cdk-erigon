package ethconfig

import (
	"time"
)

// XLayerConfig is the X Layer config used on the eth backend
type XLayerConfig struct {
	Apollo        ApolloClientConfig
	Nacos         NacosConfig
	EnableInnerTx bool
	// Sequencer
	SequencerBatchSleepDuration time.Duration
	// RPC
	DDSType  int // 0:normal(disable dds); 1:producer; 2:consumer
	DDSRedis RedisConfig
}

var DefaultXLayerConfig = XLayerConfig{}

// RedisConfig is the config for dds
type RedisConfig struct {
	Url      string // fmt: "ip:port". eg: 127.0.0.1:6379
	Password string
	DB       int
}

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
