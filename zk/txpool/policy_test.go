package txpool

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	mdbx2 "github.com/torquem-ch/mdbx-go/mdbx"
)

// newTestState creates new instance of state used by tests.
func newTestACLDB(tb testing.TB, dir string) kv.RwDB {
	tb.Helper()

	if dir == "" {
		dir = tb.TempDir()
	}

	state, err := OpenACLDB(context.Background(), dir)
	if err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			tb.Fatal(err)
		}
	})

	return state
}

func newTestTxPoolDB(tb testing.TB, dir string) kv.RwDB {
	tb.Helper()

	if dir == "" {
		dir = fmt.Sprintf("/tmp/txpool-db-temp_%v", time.Now().UTC().Format(time.RFC3339Nano))
	}

	err := os.Mkdir(dir, 0775)
	if err != nil {
		tb.Fatal(err)
	}

	txPoolDB, err := mdbx.NewMDBX(log.New()).Label(kv.TxPoolDB).Path(dir).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.TxpoolTablesCfg }).
		Flags(func(f uint) uint { return f ^ mdbx2.Durable | mdbx2.SafeNoSync }).
		GrowthStep(16 * datasize.MB).
		SyncPeriod(30 * time.Second).
		Open()
	if err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			tb.Fatal(err)
		}
	})

	return txPoolDB
}

func TestCheckDBsCreation(t *testing.T) {
	t.Parallel()

	path := fmt.Sprintf("/tmp/db-test-%v", time.Now().UTC().Format(time.RFC3339Nano))

	txPoolDB := newTestTxPoolDB(t, path)
	aclsDB := newTestACLDB(t, path)

	// Check if the dbs are created
	require.NotNil(t, txPoolDB)
	require.NotNil(t, aclsDB)
}

func TestSetMode(t *testing.T) {
	t.Parallel()

	db := newTestACLDB(t, "")
	ctx := context.Background()

	t.Run("SetMode - Valid Mode", func(t *testing.T) {
		t.Parallel()

		mode := AllowlistMode

		err := SetMode(ctx, db, mode)
		require.NoError(t, err)

		// Check if the mode is set correctly
		modeInDB, err := GetMode(ctx, db)
		require.NoError(t, err)
		require.Equal(t, string(mode), string(modeInDB))
	})

	t.Run("SetMode - Invalid Mode", func(t *testing.T) {
		t.Parallel()

		mode := "invalid_mode"

		err := SetMode(ctx, db, mode)
		require.ErrorIs(t, err, errInvalidMode)
	})
}

func TestRemovePolicy(t *testing.T) {
	t.Parallel()

	db := newTestACLDB(t, "")
	ctx := context.Background()

	SetMode(ctx, db, BlocklistMode)

	t.Run("RemovePolicy - Policy Exists", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		// Add the policy to the ACL
		require.NoError(t, AddPolicy(ctx, db, "blocklist", addr, policy))

		// Remove the policy from the ACL
		err := RemovePolicy(ctx, db, "blocklist", addr, policy)
		require.NoError(t, err)

		// Check if the policy is removed from the ACL
		hasPolicy, err := CheckPolicy(ctx, db, addr, policy)
		require.NoError(t, err)
		require.False(t, hasPolicy)
	})

	t.Run("RemovePolicy - Policy Does Not Exist", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		// Add some different policy to the ACL
		require.NoError(t, AddPolicy(ctx, db, "blocklist", addr, Deploy))

		// Remove the policy from the ACL
		err := RemovePolicy(ctx, db, "blocklist", addr, policy)
		require.NoError(t, err)

		// Check if the policy is still not present in the ACL
		hasPolicy, err := CheckPolicy(ctx, db, addr, policy)
		require.NoError(t, err)
		require.False(t, hasPolicy)
	})

	t.Run("RemovePolicy - Address Not Found", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		// Remove the policy from the ACL
		err := RemovePolicy(ctx, db, "blocklist", addr, policy)
		require.NoError(t, err)

		// Check if the policy is still not present in the ACL
		hasPolicy, err := CheckPolicy(ctx, db, addr, policy)
		require.NoError(t, err)
		require.False(t, hasPolicy)
	})

	t.Run("RemovePolicy - Unsupported acl type", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		err := RemovePolicy(ctx, db, "unknown_acl_type", addr, policy)
		require.ErrorIs(t, err, errUnsupportedACLType)
	})
}

func TestAddPolicy(t *testing.T) {
	t.Parallel()

	db := newTestACLDB(t, "")
	ctx := context.Background()

	SetMode(ctx, db, BlocklistMode)

	t.Run("AddPolicy - Policy Does Not Exist", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		err := AddPolicy(ctx, db, "blocklist", addr, policy)
		require.NoError(t, err)

		// Check if the policy exists in the ACL
		hasPolicy, err := CheckPolicy(ctx, db, addr, policy)
		require.NoError(t, err)
		require.True(t, hasPolicy)
	})

	t.Run("AddPolicy - Policy Already Exists", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		// Add the policy to the ACL
		require.NoError(t, AddPolicy(ctx, db, "blocklist", addr, policy))

		// Add the policy again
		err := AddPolicy(ctx, db, "blocklist", addr, policy)
		require.NoError(t, err)

		// Check if the policy still exists in the ACL
		hasPolicy, err := CheckPolicy(ctx, db, addr, policy)
		require.NoError(t, err)
		require.True(t, hasPolicy)
	})

	t.Run("AddPolicy - Unsupported Policy", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := Policy(33) // Assume Policy(33) is not supported

		err := AddPolicy(ctx, db, "blocklist", addr, policy)
		require.ErrorIs(t, err, errUnknownPolicy)
	})

	t.Run("AddPolicy - Unsupported acl type", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		err := AddPolicy(ctx, db, "unknown_acl_type", addr, policy)
		require.ErrorIs(t, err, errUnsupportedACLType)
	})
}

func TestUpdatePolicies(t *testing.T) {
	t.Parallel()

	db := newTestACLDB(t, "")
	ctx := context.Background()

	SetMode(ctx, db, BlocklistMode)

	t.Run("UpdatePolicies - Add Policies", func(t *testing.T) {
		t.Parallel()

		// Create test addresses and policies
		addr1 := common.HexToAddress("0x1234567890abcdef")
		addr2 := common.HexToAddress("0xabcdef1234567890")
		policies := [][]Policy{
			{SendTx, Deploy},
			{SendTx},
		}

		err := UpdatePolicies(ctx, db, "blocklist", []common.Address{addr1, addr2}, policies)
		require.NoError(t, err)

		// Check if the policies are added correctly
		hasPolicy, err := CheckPolicy(ctx, db, addr1, SendTx)
		require.NoError(t, err)
		require.True(t, hasPolicy)

		hasPolicy, err = CheckPolicy(ctx, db, addr1, Deploy)
		require.NoError(t, err)
		require.True(t, hasPolicy)

		hasPolicy, err = CheckPolicy(ctx, db, addr2, SendTx)
		require.NoError(t, err)
		require.True(t, hasPolicy)
	})

	t.Run("UpdatePolicies - Remove Policies", func(t *testing.T) {
		t.Parallel()

		// Create test addresses and policies
		addr1 := common.HexToAddress("0x1234567890abcdef")
		addr2 := common.HexToAddress("0xabcdef1234567890")
		policies := [][]Policy{
			{},
			{SendTx},
		}

		err := UpdatePolicies(ctx, db, "blocklist", []common.Address{addr1, addr2}, policies)
		require.NoError(t, err)

		// Check if the policies are removed correctly
		hasPolicy, err := CheckPolicy(ctx, db, addr1, SendTx)
		require.NoError(t, err)
		require.False(t, hasPolicy)

		hasPolicy, err = CheckPolicy(ctx, db, addr1, Deploy)
		require.NoError(t, err)
		require.False(t, hasPolicy)

		hasPolicy, err = CheckPolicy(ctx, db, addr2, SendTx)
		require.NoError(t, err)
		require.True(t, hasPolicy)
	})

	t.Run("UpdatePolicies - Empty Policies", func(t *testing.T) {
		t.Parallel()

		// Create test addresses and policies
		addr1 := common.HexToAddress("0x1234567890abcdef")
		addr2 := common.HexToAddress("0xabcdef1234567890")
		policies := [][]Policy{
			{},
			{},
		}

		err := UpdatePolicies(ctx, db, "blocklist", []common.Address{addr1, addr2}, policies)
		require.NoError(t, err)

		// Check if the policies are removed correctly
		hasPolicy, err := CheckPolicy(ctx, db, addr1, SendTx)
		require.NoError(t, err)
		require.False(t, hasPolicy)

		hasPolicy, err = CheckPolicy(ctx, db, addr1, Deploy)
		require.NoError(t, err)
		require.False(t, hasPolicy)

		hasPolicy, err = CheckPolicy(ctx, db, addr2, SendTx)
		require.NoError(t, err)
		require.False(t, hasPolicy)
	})

	t.Run("UpdatePolicies - Unsupported acl type", func(t *testing.T) {
		t.Parallel()

		// Create test addresses and policies
		addr1 := common.HexToAddress("0x1234567890abcdef")
		addr2 := common.HexToAddress("0xabcdef1234567890")
		policies := [][]Policy{
			{SendTx, Deploy},
			{SendTx},
		}

		err := UpdatePolicies(ctx, db, "unknown_acl_type", []common.Address{addr1, addr2}, policies)
		require.ErrorIs(t, err, errUnsupportedACLType)
	})
}
