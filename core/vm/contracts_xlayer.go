package vm

import (
	"time"

	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
)

var PrecompiledCache *lru.CacheWithTTL[string, []byte]

func InitPrecompiledCache(cacheSize int, ttl time.Duration) {
	PrecompiledCache = lru.NewWithTTL[string, []byte]("evm_precompiled_cache", cacheSize, ttl)
}
