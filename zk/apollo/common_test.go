package apollo

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/node/nodecfg"
)

func TestUnmarshal(t *testing.T) {

	testFilePath := "../../xlayerconfig-testnet.yaml.example"
	bytes, err := os.ReadFile(testFilePath)
	require.NoError(t, err)
	stringBytes := string(bytes)
	value := interface{}(stringBytes)

	c := &ethconfig.Config{
		Zk: &ethconfig.Zk{
			XLayer: ethconfig.XLayerConfig{
				Apollo: ethconfig.ApolloConfig{
					IP:            "0.0.0.0",
					AppID:         "xlayer-devnet",
					NamespaceName: "jsonrpc-ro.txt,jsonrpc-roHalt.properties",
					Enable:        true,
				},
			},
		},
	}
	nc := &nodecfg.Config{}
	client := NewClient(c, nc)
	nodeCfg, ethCfg, err := client.unmarshal(value)
	require.NoError(t, err)
	t.Log("http.addr: ", nodeCfg.Http.HttpListenAddress)
	t.Log("http.port: ", nodeCfg.Http.HttpPort)
	t.Log("http.api: ", nodeCfg.Http.API)
	t.Log("ws: ", nodeCfg.Http.WebsocketEnabled)
	t.Log("zkevm.apollo-enable: ", ethCfg.Zk.XLayer.Apollo.Enable)
	t.Log("zkevm.apollo-ip-addr: ", ethCfg.Zk.XLayer.Apollo.IP)
	t.Log("zkevm.apollo-app-id: ", ethCfg.Zk.XLayer.Apollo.AppID)
	t.Log("zkevm.apollo-namespace-name: ", ethCfg.Zk.XLayer.Apollo.NamespaceName)
	t.Log("zkevm.nacos-urls: ", ethCfg.Zk.XLayer.Nacos.URLs)
	t.Log("zkevm.nacos-namespace-id: ", ethCfg.Zk.XLayer.Nacos.NamespaceId)
	t.Log("zkevm.nacos-application-name: ", ethCfg.Zk.XLayer.Nacos.ApplicationName)
	t.Log("zkevm.nacos-external-listen-addr: ", ethCfg.Zk.XLayer.Nacos.ExternalListenAddr)
	t.Log("zkevm.l1-rollup-id", ethCfg.Zk.L1RollupId)
	t.Log("zkevm.l1-first-block", ethCfg.Zk.L1FirstBlock)
	t.Log("zkevm.l1-block-range", ethCfg.Zk.L1BlockRange)
	t.Log("zkevm.l1-query-delay", ethCfg.Zk.L1QueryDelay)
}
