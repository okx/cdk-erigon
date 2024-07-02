package apollo

import (
	"fmt"

	"github.com/apolloconfig/agollo/v4/storage"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

func (c *Client) loadSequencer(value interface{}) {
	ctx, err := c.getConfigContext(value)
	if err != nil {
		utils.Fatalf("load sequencer from apollo config failed, err: %v", err)
	}

	// Load sequencer config changes
	loadNodeSequencerConfig(ctx, c.nodeCfg)
	loadEthSequencerConfig(ctx, c.ethCfg)
	log.Info(fmt.Sprintf("loaded sequencer from apollo config: %+v", value.(string)))
}

// fireSequencer fires the sequencer config change
func (c *Client) fireSequencer(key string, value *storage.ConfigChange) {
	ctx, err := c.getConfigContext(value.NewValue)
	if err != nil {
		log.Error(fmt.Sprintf("fire sequencer from apollo config failed, err: %v", err))
		return
	}

	log.Info(fmt.Sprintf("apollo eth backend old config : %+v", value.OldValue.(string)))
	log.Info(fmt.Sprintf("apollo eth backend config changed: %+v", value.NewValue.(string)))

	log.Info(fmt.Sprintf("apollo node old config : %+v", value.OldValue.(string)))
	log.Info(fmt.Sprintf("apollo node config changed: %+v", value.NewValue.(string)))

	// Update sequencer node config changes
	nodecfg.UnsafeGetApolloConfig().Lock()
	nodecfg.UnsafeGetApolloConfig().EnableApollo = true
	loadNodeSequencerConfig(ctx, &nodecfg.UnsafeGetApolloConfig().Conf)
	nodecfg.UnsafeGetApolloConfig().Unlock()

	// Update sequencer eth config changes
	ethconfig.UnsafeGetApolloConfig().Lock()
	ethconfig.UnsafeGetApolloConfig().EnableApollo = true
	loadEthSequencerConfig(ctx, &ethconfig.UnsafeGetApolloConfig().Conf)
	ethconfig.UnsafeGetApolloConfig().Unlock()
}

func loadNodeSequencerConfig(ctx *cli.Context, nodeCfg *nodecfg.Config) {
	// Load sequencer config
}

func loadEthSequencerConfig(ctx *cli.Context, ethCfg *ethconfig.Config) {
	// Load sequencer config
}
