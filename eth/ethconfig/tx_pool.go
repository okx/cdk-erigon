// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethconfig

import (
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/txpool/txpoolcfg"
)

// DeprecatedTxPoolConfig are the configuration parameters of the transaction pool.
type DeprecatedTxPoolConfig struct {
	Disable  bool
	Locals   []common.Address // Addresses that should be treated by default as local
	NoLocals bool             // Whether local transaction handling should be disabled

	PriceLimit uint64 // Minimum gas price to enforce for acceptance into the pool
	PriceBump  uint64 // Minimum price bump percentage to replace an already existing transaction (nonce)

	AccountSlots uint64 // Number of executable transaction slots guaranteed per account
	GlobalSlots  uint64 // Maximum number of executable transaction slots for all accounts
	AccountQueue uint64 // Maximum number of non-executable transaction slots permitted per account
	GlobalQueue  uint64 // Maximum number of non-executable transaction slots for all accounts

	GlobalBaseFeeQueue uint64 // Maximum number of non-executable transaction slots for all accounts

	Lifetime      time.Duration // Maximum amount of time non-executable transaction are queued
	StartOnInit   bool
	TracedSenders []string // List of senders for which tx pool should print out debugging info
	CommitEvery   time.Duration

	// X Layer config
	// BlockedList is the blocked address list
	BlockedList []string
	// EnableWhitelist is a flag to enable/disable the whitelist
	EnableWhitelist bool
	// WhiteList is the white address list
	WhiteList []string
	// FreeClaimGasAddrs is the address list for claim
	FreeClaimGasAddrs []string
	// GasPriceMultiple is the factor claim tx gas price should mul
	GasPriceMultiple uint64
	// EnableFreeGasByNonce enable free gas
	EnableFreeGasByNonce bool
	// FreeGasExAddrs is the ex address which can be free gas for the transfer receiver
	FreeGasExAddrs []string
	// FreeGasCountPerAddr is the count limit of free gas tx per address
	FreeGasCountPerAddr uint64
	// FreeGasLimit is the max gas allowed use to do a free gas tx
	FreeGasLimit uint64
	// EnableFreeGasList enable the special project of XLayer for free gas
	EnableFreeGasList bool
	// FreeGasList is the special project of XLayer
	FreeGasList []FreeGasInfo
}

// FreeGasInfo contains the details for what tx should be free
type FreeGasInfo struct {
	Name             string   `json:"name"`
	FromList         []string `json:"from_list"`
	ToList           []string `json:"to_list"`
	MethodSigs       []string `json:"method_sigs"`
	GasPriceMultiple uint64   `json:"gas_price_multiple"`
}

// DeprecatedDefaultTxPoolConfig contains the default configurations for the transaction
// pool.
var DeprecatedDefaultTxPoolConfig = DeprecatedTxPoolConfig{
	PriceLimit: 1,
	PriceBump:  10,

	AccountSlots:       16,
	GlobalSlots:        10_000,
	GlobalBaseFeeQueue: 30_000,
	AccountQueue:       64,
	GlobalQueue:        30_000,

	Lifetime: 3 * time.Hour,

	// X Layer config
	BlockedList:          []string{},
	EnableWhitelist:      false,
	WhiteList:            []string{},
	FreeClaimGasAddrs:    []string{},
	GasPriceMultiple:     2,
	EnableFreeGasByNonce: false,
	FreeGasExAddrs:       []string{},
	FreeGasCountPerAddr:  3,
	FreeGasLimit:         21000,
	EnableFreeGasList:    false,
}

var DefaultTxPool2Config = func(pool1Cfg DeprecatedTxPoolConfig) txpoolcfg.Config {
	cfg := txpoolcfg.DefaultConfig
	cfg.PendingSubPoolLimit = int(pool1Cfg.GlobalSlots)
	cfg.BaseFeeSubPoolLimit = int(pool1Cfg.GlobalBaseFeeQueue)
	cfg.QueuedSubPoolLimit = int(pool1Cfg.GlobalQueue)
	cfg.PriceBump = pool1Cfg.PriceBump
	cfg.MinFeeCap = pool1Cfg.PriceLimit
	cfg.AccountSlots = pool1Cfg.AccountSlots
	cfg.LogEvery = 1 * time.Minute
	cfg.CommitEvery = 5 * time.Minute
	cfg.TracedSenders = pool1Cfg.TracedSenders
	cfg.CommitEvery = pool1Cfg.CommitEvery

	return cfg
}
