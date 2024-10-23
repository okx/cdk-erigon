package state

import (
	"encoding/json"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

type storageJson struct {
	Hash  libcommon.Hash `json:"hash"`
	Value []byte         `json:"value"`
}
type stateObjectJson struct {
	Address            libcommon.Address `json:"address"`
	Data               accounts.Account  `json:"data"`
	Original           accounts.Account  `json:"original"`
	Code               Code              `json:"code"`
	OriginStorage      []storageJson     `json:"originStorage"`
	BlockOriginStorage []storageJson     `json:"blockOriginStorage"`
	DirtyStorage       []storageJson     `json:"dirtyStorage"`
	FakeStorage        []storageJson     `json:"fakeStorage"`

	DirtyCode      bool `json:"dirtyCode"`
	Selfdestructed bool `json:"selfdestructed"`
	Deleted        bool `json:"deleted"`
	Created        bool `json:"created"`
}

func (so *stateObject) SoToJson() *stateObjectJson {
	originStorage := make([]storageJson, 0, len(so.originStorage))
	for k, v := range so.originStorage {
		originStorage = append(originStorage, storageJson{k, v.Bytes()})
	}
	blockOriginStorage := make([]storageJson, 0, len(so.blockOriginStorage))
	for k, v := range so.blockOriginStorage {
		blockOriginStorage = append(blockOriginStorage, storageJson{k, v.Bytes()})
	}
	dirtyStorage := make([]storageJson, 0, len(so.dirtyStorage))
	for k, v := range so.dirtyStorage {
		dirtyStorage = append(dirtyStorage, storageJson{k, v.Bytes()})
	}
	fakeStorage := make([]storageJson, 0, len(so.fakeStorage))
	for k, v := range so.fakeStorage {
		fakeStorage = append(fakeStorage, storageJson{k, v.Bytes()})
	}
	return &stateObjectJson{
		Address:            so.address,
		Data:               so.data,
		Original:           so.original,
		Code:               so.code,
		OriginStorage:      originStorage,
		BlockOriginStorage: blockOriginStorage,
		DirtyStorage:       dirtyStorage,
		FakeStorage:        fakeStorage,
		DirtyCode:          so.dirtyCode,
		Selfdestructed:     so.selfdestructed,
		Deleted:            so.deleted,
		Created:            so.created,
	}
}

func (soj *stateObjectJson) JsonToSo(db *IntraBlockState) (*stateObject, error) {
	originStorage := make(Storage, len(soj.OriginStorage))
	for _, ele := range soj.OriginStorage {
		var st uint256.Int
		st.SetBytes(ele.Value)
		originStorage[ele.Hash] = st
	}
	blockOriginStorage := make(Storage, len(soj.BlockOriginStorage))
	for _, ele := range soj.BlockOriginStorage {
		var st uint256.Int
		st.SetBytes(ele.Value)
		blockOriginStorage[ele.Hash] = st
	}
	dirtyStorage := make(Storage, len(soj.DirtyStorage))
	for _, ele := range soj.DirtyStorage {
		var st uint256.Int
		st.SetBytes(ele.Value)
		dirtyStorage[ele.Hash] = st
	}
	fakeStorage := make(Storage, len(soj.FakeStorage))
	for _, ele := range soj.FakeStorage {
		var st uint256.Int
		st.SetBytes(ele.Value)
		fakeStorage[ele.Hash] = st
	}
	return &stateObject{
		address:            soj.Address,
		data:               soj.Data,
		original:           soj.Original,
		code:               soj.Code,
		originStorage:      originStorage,
		blockOriginStorage: blockOriginStorage,
		dirtyStorage:       dirtyStorage,
		fakeStorage:        fakeStorage,
		dirtyCode:          soj.DirtyCode,
		selfdestructed:     soj.Selfdestructed,
		deleted:            soj.Deleted,
		created:            soj.Created,
		db:                 db,
	}, nil
}

func (soj *stateObjectJson) Marshal() ([]byte, error) {
	return json.Marshal(soj)
}

func (soj *stateObjectJson) Unmarshal(data []byte) error {
	return json.Unmarshal(data, soj)
}

type ddsData struct {
	Addr  libcommon.Address `json:"addr"`
	Data  []byte            `json:"data"`
	Dirty bool              `json:"dirty"`
}
