package apollo

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/apolloconfig/agollo/v4/storage"
	"gopkg.in/yaml.v2"

	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/log/v3"
)

func (c *Client) unmarshal(value interface{}) (nodeCfg *nodecfg.Config, ethCfg *ethconfig.Config, err error) {
	config := make(map[string]interface{})
	err = yaml.Unmarshal([]byte(value.(string)), config)
	if err != nil {
		log.Error(fmt.Sprintf("failed to load config: %v error: %v", value, err))
		return nil, nil, err
	}

	// sets global flags to value in apollo config
	ctx := createMockContext(c.flags)
	for key, value := range config {
		if !ctx.IsSet(key) {
			if reflect.ValueOf(value).Kind() == reflect.Slice {
				sliceInterface := value.([]interface{})
				s := make([]string, len(sliceInterface))
				for i, v := range sliceInterface {
					s[i] = fmt.Sprintf("%v", v)
				}
				err := ctx.Set(key, strings.Join(s, ","))
				if err != nil {
					return nil, nil, fmt.Errorf("failed setting %s flag with values=%s error=%s", key, s, err)
				}
			} else {
				err := ctx.Set(key, fmt.Sprintf("%v", value))
				if err != nil {
					return nil, nil, fmt.Errorf("failed setting %s flag with value=%v error=%s", key, value, err)
				}
			}
		}
	}

	// Apply flags to configs is fallible and thus we will need to handle failure
	defer func() {
		if r := recover(); r != nil {
			nodeCfg = nil
			ethCfg = nil
			err = fmt.Errorf("failed to unmarshal node: %v", r)
			log.Error(fmt.Sprintf("%v", err))
		}
	}()

	// Set node config. Do not set data dir and node user identification
	nodeCfg = &nodecfg.DefaultConfig
	erigoncli.ApplyFlagsForNodeConfig(ctx, nodeCfg)

	// Set eth backend config
	ethCfg = node.NewEthConfigUrfave(ctx, nodeCfg)

	return nodeCfg, ethCfg, nil
}

const (
	HaltKey         = "Halt"
	NamespaceSplits = 2
	maxHaltDelay    = 20
)

func (c *Client) fireHalt(key string, value *storage.ConfigChange) {
	switch key {
	case HaltKey:
		if value.OldValue.(string) != value.NewValue.(string) {
			random, _ := rand.Int(rand.Reader, big.NewInt(maxHaltDelay))
			delay := time.Second * time.Duration(random.Int64())
			log.Info(fmt.Sprintf("halt changed from %s to %s delay halt %v", value.OldValue.(string), value.NewValue.(string), delay))
			time.Sleep(delay)
			os.Exit(1)
		}
	}
}
