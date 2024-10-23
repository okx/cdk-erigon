package state

import (
	"encoding/json"
	"github.com/ledgerwatch/erigon/chain"
)

func (sdb *IntraBlockState) CommitBlockDDSProducer(chainRules *chain.Rules, stateWriter StateWriter) ([]byte, error) {
	delta := []ddsData{}
	success := true
	for addr := range sdb.journal.dirties {
		sdb.stateObjectsDirty[addr] = struct{}{}
	}
	for addr, obj := range sdb.stateObjects {
		if success {
			objJson := obj.SoToJson()
			if objBytes, err := objJson.Marshal(); err != nil {
				success = false
			} else {
				_, isDirty := sdb.stateObjectsDirty[addr]
				delta = append(delta, ddsData{addr, objBytes, isDirty})
			}
		}
	}

	var deltaBytes []byte
	if success {
		deltaBytes, _ = json.Marshal(&delta)
	}
	return deltaBytes, sdb.CommitBlock(chainRules, stateWriter)
}

func (sdb *IntraBlockState) CommitBlockDDSConsumer(chainRules *chain.Rules, stateWriter StateWriter, deltaBytes []byte) error {
	deltas := []ddsData{}
	if err := json.Unmarshal(deltaBytes, &deltas); err != nil {
		return err
	}
	for _, delta := range deltas {
		soJson := stateObjectJson{}
		if err := soJson.Unmarshal(delta.Data); err != nil {
			return err
		}
		so, err := soJson.JsonToSo(sdb)
		if err != nil {
			return err
		}
		sdb.stateObjects[delta.Addr] = so
		if delta.Dirty {
			sdb.stateObjectsDirty[delta.Addr] = struct{}{}
		}
	}
	return sdb.MakeWriteSet(chainRules, stateWriter)
}
