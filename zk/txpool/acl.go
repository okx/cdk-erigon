package txpool

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	mdbx2 "github.com/torquem-ch/mdbx-go/mdbx"
)

const modeKey = "mode"

type ACLTable string

const (
	Config    = "Config"
	Allowlist = "Allowlist"
	BlockList = "BlockList"
)

func (t ACLTable) String() string {
	return string(t)
}

func ResolveACLTable(table string) (ACLTable, error) {
	switch strings.ToLower(table) {
	case "config":
		return Config, nil
	case "allowlist":
		return Allowlist, nil
	case "blocklist":
		return BlockList, nil
	default:
		return "", errUnknownACLTable
	}
}

type ACLType string

const (
	AllowListType ACLType = "allowlist"
	BlockListType ACLType = "blocklist"
)

func ResolveACLType(aclType string) (ACLType, error) {
	switch strings.ToLower(aclType) {
	case "allowlist":
		return AllowListType, nil
	case "blocklist":
		return BlockListType, nil
	default:
		return "", errUnsupportedACLType
	}
}

type ACLMode string

const (
	DisabledMode  = "disabled"
	AllowlistMode = "allowlist"
	BlocklistMode = "blocklist"
)

func ResolveACLMode(mode string) (ACLMode, error) {
	switch strings.ToLower(mode) {
	case "disabled":
		return DisabledMode, nil
	case "allowlist":
		return AllowlistMode, nil
	case "blocklist":
		return BlocklistMode, nil
	default:
		return "", errInvalidMode
	}
}

var (
	ACLTables = []string{
		Config,
		Allowlist,
		BlockList,
	}

	ACLTablesCfg = kv.TableCfg{}

	errInvalidMode        = errors.New("unsupported mode")
	errUnsupportedACLType = errors.New("unsupported acl type")
	errUnknownACLTable    = errors.New("unknown acl table")
	errUnknownPolicy      = errors.New("unknown policy")
)

const ACLDB kv.Label = 255

// init initializes the ACL tables
func init() {
	for _, name := range ACLTables {
		_, ok := ACLTablesCfg[name]
		if !ok {
			ACLTablesCfg[name] = kv.TableCfgItem{}
		}
	}
}

// OpenACLDB opens the ACL database
func OpenACLDB(ctx context.Context, dbDir string) (kv.RwDB, error) {
	aclDB, err := mdbx.NewMDBX(log.New()).Label(ACLDB).Path(dbDir).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return ACLTablesCfg }).
		Flags(func(f uint) uint { return f ^ mdbx2.Durable | mdbx2.SafeNoSync }).
		GrowthStep(16 * datasize.MB).
		SyncPeriod(30 * time.Second).
		Open()
	if err != nil {
		return nil, err
	}

	return aclDB, nil
}
