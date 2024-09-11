package metrics

import (
	"time"
)

type logTag string

const (
	BlockCounter                  logTag = "BlockCounter"
	TxCounter                     logTag = "TxCounter"
	GetTx                         logTag = "GetTx"
	GetTxPauseCounter             logTag = "GetTxPauseCounter"
	BatchCloseReason              logTag = "BatchCloseReason"
	ReprocessingTxCounter         logTag = "ReProcessingTxCounter"
	FailTxCounter                 logTag = "FailTxCounter"
	FailTxResourceOverCounter     logTag = "FailTxResourceOverCounter"
	NewRound                      logTag = "NewRound"
	BatchGas                      logTag = "BatchGas"
	ProcessingTxTiming            logTag = "ProcessingTxTiming"
	ProcessingInvalidTxCounter    logTag = "ProcessingInvalidTxCounter"
	FinalizeBatchNumber           logTag = "FinalizeBatchNumber"
	BatchCommitDBTiming           logTag = "BatchCommitDBTiming"
	PbStateTiming                 logTag = "PbStateTiming"
	ZkIncIntermediateHashesTiming logTag = "ZkIncIntermediateHashesTiming"
	FinaliseBlockWriteTiming      logTag = "FinaliseBlockWriteTiming"
)

type Statistics interface {
	CumulativeCounting(tag logTag)
	CumulativeValue(tag logTag, value int64)
	CumulativeTiming(tag logTag, duration time.Duration)
	SetTag(tag logTag, value string)
	GetTag(tag logTag) string
	GetStatistics(tag logTag) int64
	Summary() string
}
