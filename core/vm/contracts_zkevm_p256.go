package vm

import (
	"github.com/ledgerwatch/erigon/crypto/secp256r1"
	"github.com/ledgerwatch/log/v3"
	"math/big"
	"sync"
)

var myMap map[[160]byte]bool

func init() {
	myMap = make(map[[160]byte]bool)
}

var mu sync.Mutex

func PadLeft(data []byte, length int) []byte {
	if len(data) < length {
		padding := make([]byte, length-len(data))
		data = append(padding, data...)
	}
	return data
}

func AddToMap(input []byte) {
	mu.Lock()
	defer mu.Unlock()
	log.Info("zjg, AddToMap---1")
	// Required input length is 160 bytes
	const p256VerifyInputLength = 160
	// Check the input length
	if len(input) != p256VerifyInputLength {
		log.Info("zjg, AddToMap---2")
		// Input length is invalid
		return
	}

	// Extract the hash, r, s, x, y from the input
	hash := input[0:32]
	r, s := new(big.Int).SetBytes(input[32:64]), new(big.Int).SetBytes(input[64:96])
	x, y := new(big.Int).SetBytes(input[96:128]), new(big.Int).SetBytes(input[128:160])

	// Verify the secp256r1 signature
	if secp256r1.Verify(hash, r, s, x, y) {
		var key [160]byte
		copy(key[:], input)
		myMap[key] = true
		log.Info("zjg, Successfully added to the map")
	} else {
		log.Info("zjg, Failed to add to the map")
	}
	log.Info("zjg, AddToMap---3")
}

func checkP256Cache(input []byte) bool {
	mu.Lock()
	defer mu.Unlock()

	var key [160]byte
	copy(key[:], input)
	if _, ok := myMap[key]; !ok {
		return false
	}
	return myMap[key]
}
