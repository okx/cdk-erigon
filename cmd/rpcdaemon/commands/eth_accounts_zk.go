package commands

import (
	"fmt"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/zkevm/hex"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/client"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/types"
	"strings"
)

func (api *APIImpl) sendGetTransactionCountToSequencer(rpcUrl string, address libcommon.Address, blockNrOrHash *rpc.BlockNumberOrHash) (*hexutil.Uint64, error) {
	addressHex := "0x" + hex.EncodeToString(address.Bytes())
	var blockNrOrHashValue interface{}
	if blockNrOrHash != nil {
		if blockNrOrHash.BlockNumber != nil {
			bn := *blockNrOrHash.BlockNumber
			blockNrOrHashValue = bn.MarshallJson()
		} else if blockNrOrHash.BlockHash != nil {
			blockNrOrHashValue = "0x" + hex.EncodeToString(blockNrOrHash.BlockHash.Bytes())
		}
	}

	res, err := client.JSONRPCCallWhitLimit(types.L2RpcLimit{rpcUrl, api.l2RpcLimit}, rpcUrl, "eth_getTransactionCount", addressHex, blockNrOrHashValue)
	if err != nil {
		return nil, err
	}

	if res.Error != nil {
		return nil, fmt.Errorf("RPC error response: %s", res.Error.Message)
	}

	//hash comes in escaped quotes, so we trim them here
	// \"0x1234\" -> 0x1234
	hashHex := strings.Trim(string(res.Result), "\"")

	// now convert to a uint
	decoded, err := hexutil.DecodeUint64(hashHex)
	if err != nil {
		return nil, err
	}

	result := hexutil.Uint64(decoded)

	return &result, nil
}
