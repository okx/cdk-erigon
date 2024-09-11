// Hook go-metrics into expvar
// on any /debug/metrics request, load all vars from the registry into expvar, and execute regular expvar handler
package exp

import (
	"fmt"
	"net/http"

	"github.com/VictoriaMetrics/metrics"
	"github.com/ledgerwatch/log/v3"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Setup starts a dedicated metrics server at the given address.
// This function enables metrics reporting separate from pprof.
func Setup(address string) {
	// For X Layer
	mux := http.NewServeMux()
	mux.Handle("/debug/metrics/prometheus", promhttp.Handler())
	mux.Handle("/debug/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		metrics.WritePrometheus(w, true)
	}))
	log.Info("Starting metrics server", "addr", fmt.Sprintf("http://%v/debug/metrics/prometheus or http://%v/debug/metrics", address, address))
	go func() {
		if err := http.ListenAndServe(address, mux); err != nil { // nolint:gosec
			log.Error("Failure in running metrics server", "err", err)
		}
	}()
}
