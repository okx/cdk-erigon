package apollo

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Duration is a wrapper type that parses time duration from text.
type Duration struct {
	time.Duration `validate:"required"`
}

type RPCConfig struct {
	// Host defines the network adapter that will be used to serve the HTTP requests
	Host string `mapstructure:"Host"`

	// Port defines the port to serve the endpoints via HTTP
	Port int `mapstructure:"Port"`

	// ReadTimeout is the HTTP server read timeout
	// check net/http.server.ReadTimeout and net/http.server.ReadHeaderTimeout
	ReadTimeout Duration `mapstructure:"ReadTimeout"`

	// WriteTimeout is the HTTP server write timeout
	// check net/http.server.WriteTimeout
	WriteTimeout Duration `mapstructure:"WriteTimeout"`

	// MaxRequestsPerIPAndSecond defines how much requests a single IP can
	// send within a single second
	MaxRequestsPerIPAndSecond float64 `mapstructure:"MaxRequestsPerIPAndSecond"`

	// SequencerNodeURI is used allow Non-Sequencer nodes
	// to relay transactions to the Sequencer node
	SequencerNodeURI string `mapstructure:"SequencerNodeURI"`

	// MaxCumulativeGasUsed is the max gas allowed per batch
	MaxCumulativeGasUsed uint64

	// WebSockets configuration
	WebSockets WebSocketsConfig `mapstructure:"WebSockets"`

	// EnableL2SuggestedGasPricePolling enables polling of the L2 gas price to block tx in the RPC with lower gas price.
	EnableL2SuggestedGasPricePolling bool `mapstructure:"EnableL2SuggestedGasPricePolling"`

	// BatchRequestsEnabled defines if the Batch requests are enabled or disabled
	BatchRequestsEnabled bool `mapstructure:"BatchRequestsEnabled"`

	// BatchRequestsLimit defines the limit of requests that can be incorporated into each batch request
	BatchRequestsLimit uint `mapstructure:"BatchRequestsLimit"`

	// L2Coinbase defines which address is going to receive the fees
	L2Coinbase common.Address

	// MaxLogsCount is a configuration to set the max number of logs that can be returned
	// in a single call to the state, if zero it means no limit
	MaxLogsCount uint64 `mapstructure:"MaxLogsCount"`

	// MaxLogsBlockRange is a configuration to set the max range for block number when querying TXs
	// logs in a single call to the state, if zero it means no limit
	MaxLogsBlockRange uint64 `mapstructure:"MaxLogsBlockRange"`

	// MaxNativeBlockHashBlockRange is a configuration to set the max range for block number when querying
	// native block hashes in a single call to the state, if zero it means no limit
	MaxNativeBlockHashBlockRange uint64 `mapstructure:"MaxNativeBlockHashBlockRange"`

	// EnableHttpLog allows the user to enable or disable the logs related to the HTTP
	// requests to be captured by the server.
	EnableHttpLog bool `mapstructure:"EnableHttpLog"`

	// ZKCountersLimits defines the ZK Counter limits
	ZKCountersLimits ZKCountersLimits

	// XLayer config
	// EnablePendingTransactionFilter enables pending transaction filter that can support query L2 pending transaction
	EnablePendingTransactionFilter bool `mapstructure:"EnablePendingTransactionFilter"`

	// Nacos configuration
	Nacos NacosConfig `mapstructure:"Nacos"`

	// NacosWs configuration
	NacosWs NacosConfig `mapstructure:"NacosWs"`

	// GasLimitFactor is used to multiply the suggested gas provided by the network
	// in order to allow a enough gas to be set for all the transactions default value is 1.
	//
	// ex:
	// suggested gas limit: 100
	// GasLimitFactor: 1
	// gas limit = 100
	//
	// suggested gas limit: 100
	// GasLimitFactor: 1.1
	// gas limit = 110
	GasLimitFactor float64 `mapstructure:"GasLimitFactor"`

	// DisableAPIs disable some API
	DisableAPIs []string `mapstructure:"DisableAPIs"`

	// RateLimit enable rate limit
	RateLimit RateLimitConfig `mapstructure:"RateLimit"`

	// DynamicGP defines the config of dynamic gas price
	DynamicGP DynamicGPConfig `mapstructure:"DynamicGP"`

	// EnableInnerTxCacheDB enables the inner tx cache db
	EnableInnerTxCacheDB bool `mapstructure:"EnableInnerTxCacheDB"`

	// BridgeAddress is the address of the bridge contract
	BridgeAddress common.Address `mapstructure:"BridgeAddress"`

	// ApiAuthentication defines the authentication configuration for the API
	ApiAuthentication ApiAuthConfig `mapstructure:"ApiAuthentication"`

	// ApiRelay defines the relay configuration for the API
	ApiRelay ApiRelayConfig `mapstructure:"ApiRelay"`
}
