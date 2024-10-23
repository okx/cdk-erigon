package stages

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gateway-fm/cdk-erigon-lib/common/length"
	"github.com/go-redis/redis/v8"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/status-im/keycard-go/hexutils"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	state2 "github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/log/v3"
)

func zkIncrementIntermediateHashesDDSConsumer(ctx context.Context, logPrefix string, s *stagedsync.StageState, db kv.RwTx, eridb *db2.EriDb, dbSmt *smt.SMT, from, to uint64) (common.Hash, error) {
	log.Info(fmt.Sprintf("[%s] Increment trie hashes started", logPrefix), "previousRootHeight", s.BlockNumber, "calculatingRootHeight", to)
	defer log.Info(fmt.Sprintf("[%s] Increment ended", logPrefix))

	ac, err := db.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer ac.Close()

	sc, err := db.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer sc.Close()

	// progress printer
	accChanges := make(map[common.Address]*accounts.Account)
	codeChanges := make(map[common.Address]string)
	storageChanges := make(map[common.Address]map[string]string)

	// case when we are incrementing from block 1
	// we chould include the 0 block which is the genesis data
	if from != 0 {
		from += 1
	}

	// NB: changeset tables are zero indexed
	// changeset tables contain historical value at N-1, so we look up values from plainstate
	// i+1 to get state at the beginning of the next batch
	psr := state2.NewPlainState(db, from+1, systemcontracts.SystemContractCodeLookup["Hermez"])
	defer psr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "192.168.1.19:6379", // Redis 服务器地址
		Password: "",                  // Redis 密码（如果没有密码，可以省略或留空）
		DB:       0,                   // Redis 数据库编号，默认是 0
	})

	for i := from; i <= to; i++ {
		dupSortKey := dbutils.EncodeBlockNumber(i)
		psr.SetBlockNr(i + 1)

		useDDS, err := rdb.Get(context.Background(), fmt.Sprintf("useDDS_%d", i)).Bool()
		if err != nil {
			panic("get useDDS fail")
		}
		if useDDS {
			accBytes, err := rdb.Get(context.Background(), fmt.Sprintf("acc_%d", i)).Bytes()
			if err != nil {
				panic("get accChanges err")
			}
			accChangesI := make(map[common.Address]*accounts.Account)
			if err := json.Unmarshal(accBytes, &accChangesI); err != nil {
				panic("unmarshal accChanges err")
			}
			for k, v := range accChangesI {
				accChanges[k] = v
			}

			codeBytes, err := rdb.Get(context.Background(), fmt.Sprintf("code_%d", i)).Bytes()
			if err != nil {
				panic("get codeChanges err")
			}
			codeChangesI := make(map[common.Address]string)
			if err := json.Unmarshal(codeBytes, &codeChangesI); err != nil {
				panic("unmarshal codeChanges err")
			}
			for k, v := range codeChangesI {
				codeChanges[k] = v
			}

			storageBytes, err := rdb.Get(context.Background(), fmt.Sprintf("storage_%d", i)).Bytes()
			if err != nil {
				panic("get storageChanges err")
			}
			storageChangesI := make(map[common.Address]map[string]string)
			if err := json.Unmarshal(storageBytes, &storageChangesI); err != nil {
				panic("unmarshal storageChanges err")
			}
			for k, v := range storageChangesI {
				for mk, mv := range v {
					if storageChanges[k] == nil {
						storageChanges[k] = make(map[string]string)
					}
					storageChanges[k][mk] = mv
				}
			}
		} else {
			// collect changes to accounts and code
			for _, v, err := ac.SeekExact(dupSortKey); err == nil && v != nil; _, v, err = ac.NextDup() {
				addr := common.BytesToAddress(v[:length.Addr])

				currAcc, err := psr.ReadAccountData(addr)
				if err != nil {
					return trie.EmptyRoot, err
				}

				// store the account
				accChanges[addr] = currAcc

				cc, err := psr.ReadAccountCode(addr, currAcc.Incarnation, currAcc.CodeHash)
				if err != nil {
					return trie.EmptyRoot, err
				}

				ach := hexutils.BytesToHex(cc)
				if len(ach) > 0 {
					hexcc := "0x" + ach
					codeChanges[addr] = hexcc
					if err != nil {
						return trie.EmptyRoot, err
					}
				}
			}

			err = db.ForPrefix(kv.StorageChangeSet, dupSortKey, func(sk, sv []byte) error {
				changesetKey := sk[length.BlockNum:]
				address, incarnation := dbutils.PlainParseStoragePrefix(changesetKey)

				sstorageKey := sv[:length.Hash]
				stk := common.BytesToHash(sstorageKey)

				value, err := psr.ReadAccountStorage(address, incarnation, &stk)
				if err != nil {
					return err
				}

				stkk := fmt.Sprintf("0x%032x", stk)
				v := fmt.Sprintf("0x%032x", common.BytesToHash(value))

				m := make(map[string]string)
				m[stkk] = v

				if storageChanges[address] == nil {
					storageChanges[address] = make(map[string]string)
				}
				storageChanges[address][stkk] = v
				return nil
			})
			if err != nil {
				return trie.EmptyRoot, err
			}
		}

	}

	log.Info(fmt.Sprintf("SetStorage!!!! %d", to))
	if _, _, err := dbSmt.SetStorage(ctx, logPrefix, accChanges, codeChanges, storageChanges); err != nil {
		return trie.EmptyRoot, err
	}

	log.Info(fmt.Sprintf("[%s] Regeneration trie hashes finished. Commiting batch", logPrefix))

	lr := dbSmt.LastRoot()

	hash := common.BigToHash(lr)

	// do not put this outside, because sequencer uses this function to calculate root for each block
	hermezDb := hermez_db.NewHermezDb(db)
	if err := hermezDb.WriteSmtDepth(to, uint64(dbSmt.GetDepth())); err != nil {
		return trie.EmptyRoot, err
	}

	return hash, nil
}

func zkIncrementIntermediateHashesDDSProducer(ctx context.Context, logPrefix string, s *stagedsync.StageState, db kv.RwTx, eridb *db2.EriDb, dbSmt *smt.SMT, from, to uint64) (common.Hash, error) {
	log.Info(fmt.Sprintf("[%s] Increment trie hashes started", logPrefix), "previousRootHeight", s.BlockNumber, "calculatingRootHeight", to)
	defer log.Info(fmt.Sprintf("[%s] Increment ended", logPrefix))

	ac, err := db.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer ac.Close()

	sc, err := db.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer sc.Close()

	// progress printer
	accChanges := make(map[common.Address]*accounts.Account)
	codeChanges := make(map[common.Address]string)
	storageChanges := make(map[common.Address]map[string]string)

	// case when we are incrementing from block 1
	// we chould include the 0 block which is the genesis data
	if from != 0 {
		from += 1
	}

	// NB: changeset tables are zero indexed
	// changeset tables contain historical value at N-1, so we look up values from plainstate
	// i+1 to get state at the beginning of the next batch
	psr := state2.NewPlainState(db, from+1, systemcontracts.SystemContractCodeLookup["Hermez"])
	defer psr.Close()

	for i := from; i <= to; i++ {
		accChangesI := make(map[common.Address]*accounts.Account)
		codeChangesI := make(map[common.Address]string)
		storageChangesI := make(map[common.Address]map[string]string)
		dupSortKey := dbutils.EncodeBlockNumber(i)
		psr.SetBlockNr(i + 1)

		// collect changes to accounts and code
		for _, v, err := ac.SeekExact(dupSortKey); err == nil && v != nil; _, v, err = ac.NextDup() {
			addr := common.BytesToAddress(v[:length.Addr])

			currAcc, err := psr.ReadAccountData(addr)
			if err != nil {
				return trie.EmptyRoot, err
			}

			// store the account
			accChanges[addr] = currAcc
			accChangesI[addr] = currAcc

			cc, err := psr.ReadAccountCode(addr, currAcc.Incarnation, currAcc.CodeHash)
			if err != nil {
				return trie.EmptyRoot, err
			}

			ach := hexutils.BytesToHex(cc)
			if len(ach) > 0 {
				hexcc := "0x" + ach
				codeChanges[addr] = hexcc
				codeChangesI[addr] = hexcc
				if err != nil {
					return trie.EmptyRoot, err
				}
			}
		}

		err = db.ForPrefix(kv.StorageChangeSet, dupSortKey, func(sk, sv []byte) error {
			changesetKey := sk[length.BlockNum:]
			address, incarnation := dbutils.PlainParseStoragePrefix(changesetKey)

			sstorageKey := sv[:length.Hash]
			stk := common.BytesToHash(sstorageKey)

			value, err := psr.ReadAccountStorage(address, incarnation, &stk)
			if err != nil {
				return err
			}

			stkk := fmt.Sprintf("0x%032x", stk)
			v := fmt.Sprintf("0x%032x", common.BytesToHash(value))

			m := make(map[string]string)
			m[stkk] = v

			if storageChanges[address] == nil {
				storageChanges[address] = make(map[string]string)
			}
			if storageChangesI[address] == nil {
				storageChangesI[address] = make(map[string]string)
			}
			storageChanges[address][stkk] = v
			storageChangesI[address][stkk] = v
			return nil
		})
		if err != nil {
			return trie.EmptyRoot, err
		}

		rdb := redis.NewClient(&redis.Options{
			Addr:     "192.168.1.19:6379", // Redis 服务器地址
			Password: "",                  // Redis 密码（如果没有密码，可以省略或留空）
			DB:       0,                   // Redis 数据库编号，默认是 0
		})
		accBytes, err := json.Marshal(accChangesI)
		if err != nil {
			log.Error("marshal accChanges err")
		}
		codeBytes, err := json.Marshal(codeChangesI)
		if err != nil {
			log.Error("marshal codeChanges err")
		}
		storageBytes, err := json.Marshal(storageChangesI)
		if err != nil {
			log.Error("marshal storageChanges err")
		}
		rdb.Set(context.Background(), fmt.Sprintf("acc_%d", i), accBytes, 0)
		rdb.Set(context.Background(), fmt.Sprintf("code_%d", i), codeBytes, 0)
		rdb.Set(context.Background(), fmt.Sprintf("storage_%d", i), storageBytes, 0)

	}

	if _, _, err := dbSmt.SetStorage(ctx, logPrefix, accChanges, codeChanges, storageChanges); err != nil {
		return trie.EmptyRoot, err
	}

	log.Info(fmt.Sprintf("[%s] Regeneration trie hashes finished. Commiting batch", logPrefix))

	lr := dbSmt.LastRoot()

	hash := common.BigToHash(lr)

	// do not put this outside, because sequencer uses this function to calculate root for each block
	hermezDb := hermez_db.NewHermezDb(db)
	if err := hermezDb.WriteSmtDepth(to, uint64(dbSmt.GetDepth())); err != nil {
		return trie.EmptyRoot, err
	}

	return hash, nil
}
