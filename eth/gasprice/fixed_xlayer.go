package gasprice

import (
	"context"
	"fmt"
	"math/big"
	"strconv"

	"github.com/ledgerwatch/erigon/eth/gasprice/gaspricecfg"
	"github.com/ledgerwatch/log/v3"
)

// FixedGasPrice struct
type FixedGasPrice struct {
	cfg       gaspricecfg.Config
	ctx       context.Context
	lastRawGP *big.Int
	ratePrc   *KafkaProcessor
}

// newFixedGasPriceSuggester inits l2 fixed price suggester.
func newFixedGasPriceSuggester(ctx context.Context, cfg gaspricecfg.Config) *FixedGasPrice {
	gps := &FixedGasPrice{
		cfg:       cfg,
		ctx:       ctx,
		lastRawGP: new(big.Int).Set(cfg.Default),
		ratePrc:   newKafkaProcessor(cfg.XLayer, ctx),
	}
	return gps
}

// UpdateGasPriceAvg updates the gas price.
func (f *FixedGasPrice) UpdateGasPriceAvg(l1GasPrice *big.Int) {
	// Get L2 coin price
	l2CoinPrice := f.ratePrc.GetL2CoinPrice()
	if l2CoinPrice < minUSDTPrice {
		log.Warn("update gas price average failed, the L2 native coin price is too small")
		return
	}

	// Convert fixed gas price in USDT to OKB
	res := big.NewFloat(0).SetFloat64(f.cfg.XLayer.GasPriceUsdt / l2CoinPrice)
	// Convert fixed gas price to OKBWei
	result := OKBToOKBWei(res)

	// Check for min/max L2 gasPrice
	minGasPrice := new(big.Int).Set(f.cfg.Default)
	if minGasPrice.Cmp(result) == 1 { // minGasPrice > result
		log.Warn(fmt.Sprintf("Fixed mode, setting DefaultGasPrice for L2: %s, result:%v", f.cfg.Default.String(), result.String()))
		result = minGasPrice
	}
	maxGasPrice := new(big.Int).Set(f.cfg.MaxPrice)
	if maxGasPrice.Int64() > 0 && result.Cmp(maxGasPrice) == 1 { // result > maxGasPrice
		log.Warn("setting MaxGasPriceWei for L2")
		result = maxGasPrice
	}
	var truncateValue *big.Int
	log.Debug(fmt.Sprintf("Full L2 gas price value: %s. Length: %d. L1 gas price: %s", result.String(), len(result.String()), l1GasPrice.String()))
	numLength := len(result.String())
	if numLength > 3 { //nolint:gomnd
		aux := "%0" + strconv.Itoa(numLength-3) + "d" //nolint:gomnd
		var ok bool
		value := result.String()[:3] + fmt.Sprintf(aux, 0)
		truncateValue, ok = new(big.Int).SetString(value, 10)
		if !ok {
			log.Error(fmt.Sprintf("error converting: %s", value))
		}
	} else {
		truncateValue = result
	}

	// Cache L2 gasPrice calculated
	log.Debug(fmt.Sprintf("Storing truncated L2 gas price: %s, L2 native coin price: %g.", truncateValue.String(), l2CoinPrice))
	if truncateValue != nil {
		log.Info(fmt.Sprintf("Set l2 raw gas price: %d", truncateValue.Uint64()))
		f.lastRawGP = truncateValue
	} else {
		log.Error("nil value detected. Skipping...")
	}
}

func (f *FixedGasPrice) UpdateConfig(c gaspricecfg.Config) {
	f.cfg = c
}

func (f *FixedGasPrice) GetLastRawGP() *big.Int {
	return f.lastRawGP
}

func (f *FixedGasPrice) GetConfig() gaspricecfg.Config {
	return f.cfg
}

func (f *FixedGasPrice) GetCtx() context.Context {
	return f.ctx
}
