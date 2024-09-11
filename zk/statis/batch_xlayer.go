package statis

import (
	"fmt"
	"sync"
	"time"

	"github.com/ledgerwatch/log/v3"
)

var batchLogger *batchLogInstance
var batchOnce sync.Once

type batchLogInstance struct {
	BatchNum      uint64
	BlockCount    uint64
	TxCount       uint64
	ClosingReason string
	TotalDuration time.Duration
}

func BatchLogger() *batchLogInstance {
	batchOnce.Do(func() {
		batchLogger = &batchLogInstance{}
		batchLogger.init()
	})
	return batchLogger
}

func (b *batchLogInstance) init() {
	b.BatchNum = 0
	b.BlockCount = 0
	b.TxCount = 0
	b.ClosingReason = ""
	b.TotalDuration = 0
}

func (b *batchLogInstance) SetBlockNum(batchNum uint64) {
	b.BatchNum = batchNum
}

func (b *batchLogInstance) AccumulateTxCount(txCount uint64) {
	b.TxCount += txCount
}

func (b *batchLogInstance) AccumulateBlockCount() {
	b.BlockCount += 1
}

func (b *batchLogInstance) SetClosingReason(closingReason string) {
	b.ClosingReason = closingReason
}

func (b *batchLogInstance) SetTotalDuration(totalDuration time.Duration) {
	b.TotalDuration = totalDuration
}

func (b *batchLogInstance) PrintLogAndFlush() {
	itemLog := fmt.Sprintf("[Statis Batch] <%v>, ClosingReason<%v>, BlockCount<%v>, TxCount<%v>, TotalDuration<%vms>", b.BatchNum, b.ClosingReason, b.BlockCount, b.TxCount, b.TotalDuration.Milliseconds())
	log.Info(itemLog)

	b.init()
}
