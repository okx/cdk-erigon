package statis

import (
	"fmt"
	"sync"
	"time"

	"github.com/ledgerwatch/log/v3"
)

const (
	AddTxs                  = "AddTxs"
	WaitBlockTimeOut        = "WaitBlockTimeOut"
	WaitBatchTimeOut        = "WaitBatchTimeOut"
	PbState                 = "PbState"
	ZkIncIntermediateHashes = "ZkIncIntermediateHashes"
	FinaliseBlockWrite      = "FinaliseBlockWrite"
	Save2DB                 = "commitToDB"
)

var blockLogger *blockLogInstance
var blockOnce sync.Once

type blockLogInstance struct {
	BlockNum      uint64
	TxCount       uint64
	TotalDuration time.Duration
	StepLog       string
	ClosingReason string
}

func BlockLogger() *blockLogInstance {
	blockOnce.Do(func() {
		blockLogger = &blockLogInstance{}
		blockLogger.init()
	})
	return blockLogger
}

func (b *blockLogInstance) init() {
	b.BlockNum = 0
	b.TxCount = 0
	b.TotalDuration = 0
	b.StepLog = ""
	b.ClosingReason = ""
}

func (b *blockLogInstance) SetBlockNum(blockNum uint64) {
	b.BlockNum = blockNum
}

func (b *blockLogInstance) SetClosingReason(closingReason string) {
	b.ClosingReason = closingReason
}

func (b *blockLogInstance) SetTxCount(txCount uint64) {
	b.TxCount = txCount
}

func (b *blockLogInstance) SetTotalDuration(totalDuration time.Duration) {
	b.TotalDuration = totalDuration
}

func (b *blockLogInstance) AppendStepLog(stepTag string, stepDuration time.Duration) {
	b.StepLog = b.StepLog + ", " + fmt.Sprintf("%v<%vms>", stepTag, stepDuration.Milliseconds())
}

func (b *blockLogInstance) PrintLogAndFlush() {
	itemLog := fmt.Sprintf("[Statis Block] <%v>, TxCount<%v>, TotalDuration<%vms>", b.BlockNum, b.TxCount, b.TotalDuration.Milliseconds())
	overallLog := itemLog + b.StepLog
	log.Info(overallLog)

	b.init()
}
