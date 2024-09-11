package metrics

import (
	"strconv"
	"sync"
	"time"
)

var instance *statisticsInstance
var once sync.Once

// GetLogStatistics is get log instance for statistic
func GetLogStatistics() Statistics {
	once.Do(func() {
		instance = &statisticsInstance{}
		instance.init()
	})
	return instance
}

type statisticsInstance struct {
	timestamp  map[logTag]time.Time
	statistics map[logTag]int64 // value maybe the counter or time.Duration(ms)
	tags       map[logTag]string
}

func (l *statisticsInstance) init() {
	l.timestamp = make(map[logTag]time.Time)
	l.statistics = make(map[logTag]int64)
	l.tags = make(map[logTag]string)
}

func (l *statisticsInstance) CumulativeCounting(tag logTag) {
	l.statistics[tag]++
}

func (l *statisticsInstance) CumulativeValue(tag logTag, value int64) {
	l.statistics[tag] += value
}

func (l *statisticsInstance) CumulativeTiming(tag logTag, duration time.Duration) {
	l.statistics[tag] += duration.Milliseconds()
}

func (l *statisticsInstance) SetTag(tag logTag, value string) {
	l.tags[tag] = value
}

func (l *statisticsInstance) UpdateTimestamp(tag logTag, tm time.Time) {
	l.timestamp[tag] = tm
}

func (l *statisticsInstance) ResetStatistics() {
	l.statistics = make(map[logTag]int64)
	l.tags = make(map[logTag]string)
}

func (l *statisticsInstance) Summary() string {
	batchTotalDuration := "-"
	if key, ok := l.timestamp[NewRound]; ok {
		batchTotalDuration = strconv.Itoa(int(time.Since(key).Milliseconds()))
	}

	batch := "Batch<" + l.tags[FinalizeBatchNumber] + ">, "
	totalDuration := "TotalDuration<" + batchTotalDuration + "ms>, "
	gasUsed := "GasUsed<" + strconv.Itoa(int(l.statistics[BatchGas])) + ">, "
	blockCount := "Block<" + strconv.Itoa(int(l.statistics[BlockCounter])) + ">, "
	tx := "Tx<" + strconv.Itoa(int(l.statistics[TxCounter])) + ">, "
	getTx := "GetTx<" + strconv.Itoa(int(l.statistics[GetTx])) + "ms>, "
	getTxPause := "GetTxPause<" + strconv.Itoa(int(l.statistics[GetTxPauseCounter])) + ">, "
	reprocessTx := "ReprocessTx<" + strconv.Itoa(int(l.statistics[ReprocessingTxCounter])) + ">, "
	resourceOverTx := "ResourceOverTx<" + strconv.Itoa(int(l.statistics[FailTxResourceOverCounter])) + ">, "
	failTx := "FailTx<" + strconv.Itoa(int(l.statistics[FailTxCounter])) + ">, "
	invalidTx := "InvalidTx<" + strconv.Itoa(int(l.statistics[ProcessingInvalidTxCounter])) + ">, "
	processTxTiming := "ProcessTx<" + strconv.Itoa(int(l.statistics[ProcessingTxTiming])) + "ms>, "
	batchCommitDBTiming := "BatchCommitDBTiming<" + strconv.Itoa(int(l.statistics[BatchCommitDBTiming])) + "ms>, "
	pbStateTiming := "PbStateTiming<" + strconv.Itoa(int(l.statistics[PbStateTiming])) + "ms>, "
	zkIncIntermediateHashesTiming := "ZkIncIntermediateHashesTiming<" + strconv.Itoa(int(l.statistics[ZkIncIntermediateHashesTiming])) + "ms>, "
	finaliseBlockWriteTiming := "FinaliseBlockWriteTiming<" + strconv.Itoa(int(l.statistics[FinaliseBlockWriteTiming])) + "ms>, "
	batchCloseReason := "BatchCloseReason<" + l.tags[BatchCloseReason] + ">"

	result := batch + totalDuration + gasUsed + blockCount + tx + getTx + getTxPause +
		reprocessTx + resourceOverTx + failTx + invalidTx + processTxTiming + pbStateTiming +
		zkIncIntermediateHashesTiming + finaliseBlockWriteTiming + batchCommitDBTiming +
		batchCloseReason

	return result
}

func (l *statisticsInstance) GetTag(tag logTag) string {
	return l.tags[tag]
}

func (l *statisticsInstance) GetStatistics(tag logTag) int64 {
	return l.statistics[tag]
}
