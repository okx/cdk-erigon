package apollo

import (
	"fmt"

	"github.com/apolloconfig/agollo/v4/storage"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/log/v3"
)

func (c *Client) loadL2GasPricer(value interface{}) {
	_, _, err := c.unmarshal(value)
	if err != nil {
		utils.Fatalf("load l2gaspricer from apollo config failed, unmarshal err: %v", err)
	}

	// TODO: Add specific l2gaspricer configs to load from apollo config
	log.Info(fmt.Sprintf("loaded l2gaspricer from apollo config: %+v", value.(string)))
}

// fireL2GasPricer fires the l2gaspricer config change
func (c *Client) fireL2GasPricer(key string, value *storage.ConfigChange) {
	nodeCfg, ethCfg, err := c.unmarshal(value.NewValue)
	if err != nil {
		log.Error(fmt.Sprintf("fire l2gaspricer from apollo config failed, unmarshal err: %v", err))
		return
	}

	log.Info(fmt.Sprintf("apollo eth backend old config : %+v", value.OldValue.(string)))
	log.Info(fmt.Sprintf("apollo eth backend config changed: %+v", value.NewValue.(string)))

	log.Info(fmt.Sprintf("apollo node old config : %+v", value.OldValue.(string)))
	log.Info(fmt.Sprintf("apollo node config changed: %+v", value.NewValue.(string)))

	nodecfg.UpdateL2GasPricerConfig(*nodeCfg)
	ethconfig.UpdateL2GasPricerConfig(*ethCfg)
}
