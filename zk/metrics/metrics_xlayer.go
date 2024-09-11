package metrics

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/prometheus/client_golang/prometheus"
)

type BatchFinalizeType string

const (
	BatchTimeOut         BatchFinalizeType = "EmptyBatchTimeOut"
	BatchCounterOverflow BatchFinalizeType = "BatchCounterOverflow"
	BatchLimboRecovery   BatchFinalizeType = "LimboRecovery"
)

var (
	SeqPrefix            = "sequencer_"
	BatchExecuteTimeName = SeqPrefix + "batch_execute_time"
	PoolTxCountName      = SeqPrefix + "pool_tx_count"
)

func Init() {
	prometheus.MustRegister(BatchExecuteTimeGauge)
	prometheus.MustRegister(PoolTxCount)
}

var BatchExecuteTimeGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: BatchExecuteTimeName,
		Help: "[SEQUENCER] batch execution time in second",
	},
	[]string{"closingReason"},
)

var PoolTxCount = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: PoolTxCountName,
		Help: "[SEQUENCER] tx count of each pool in tx pool",
	},
	[]string{"poolName"},
)

// BatchExecuteTime sets the gauge vector to the given batch type and time.
func BatchExecuteTime(closingReason string, duration time.Duration) {
	log.Info(fmt.Sprintf("[BatchExecuteTime] ClosingReason: %s, Duration: %.2fs", closingReason, duration.Seconds()))
	BatchExecuteTimeGauge.WithLabelValues(closingReason).Set(duration.Seconds())
}

func AddPoolTxCount(pending, baseFee, queued int) {
	log.Info(fmt.Sprintf("[PoolTxCount] pending: %d, basefee: %d, queued: %d", pending, baseFee, queued))
	PoolTxCount.WithLabelValues("pending").Set(float64(pending))
	PoolTxCount.WithLabelValues("basefee").Set(float64(baseFee))
	PoolTxCount.WithLabelValues("queued").Set(float64(queued))
}
