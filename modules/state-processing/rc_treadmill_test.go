package state_engine_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	rcDb "vsc-node/modules/db/vsc/rcs"

	"github.com/vsc-eco/hivego"
)

// RC-TREADMILL-1: a Hive account that has never held HBD has the 10,000 RC free
// allowance and nothing else. Every failure path charges a flat RC without
// asking whether the payer can pay it, and the clamp that stops the stored
// frozen total exceeding the account's ceiling re-stamps its RC record at the
// closing slot — re-anchoring the RC_RETURN_PERIOD schedule for the whole
// total. Once the total is pinned at the ceiling the account is charged RC it
// does not have, and 10,000 RC returning at 0.069 RC a block truncates to
// nothing under a re-anchor less than 15 blocks later: its RC never came back
// while it kept retrying. From 0.18.0 an uncharged slot leaves the record alone.
//
// These drive real L1 blocks through the engine: a vsc.call anchored on Hive has
// no mempool to reject it for want of RC (that gate is for offchain txs only,
// transaction-pool.go), so it is always included, always fails, and was always
// charged.

// rcTreadmillRetry is one L1 vsc.call from alice to a contract that does not
// exist: the engine refuses it and charges the flat failure RC. The null blocks
// close the slot so UpdateRcMap folds the charge into alice's RC record.
func rcTreadmillRetry(te *testEnv, txId string) {
	call, _ := json.Marshal(map[string]interface{}{
		"net_id":      "vsc-mocknet",
		"contract_id": "vsc1NoSuchContract",
		"action":      "execute",
		"payload":     "{}",
		"rc_limit":    1000,
		"intents":     []interface{}{},
	})
	te.Creator.BroadcastOps([]hivego.Operation{
		{
			Type: "custom_json",
			Value: map[string]interface{}{
				"id":                     "vsc.call",
				"json":                   string(call),
				"required_auths":         []string{"alice"},
				"required_posting_auths": []string{},
			},
		},
	}, txId)
	te.processAndWait()
	te.Reader.MineNullBlocks(10)
	time.Sleep(200 * time.Millisecond)
}

// rcRecords is every RC record the engine wrote for the account, in the order
// it wrote them.
func rcRecords(te *testEnv, account string) []rcDb.RcRecord {
	return te.RcDb.Records[account]
}

// rcTreadmillEnv is alice with no HBD and her free allowance already spent:
// one RC record, the full allowance frozen, anchored at height 1.
func rcTreadmillEnv(t *testing.T, consensus uint64) (*testEnv, int64) {
	t.Helper()
	te := newTestEnv()
	te.ElectionDb.ElectionsByHeight[1] = versionedElection(1, 1, 0, consensus)
	free := te.SE.RcSystem.HiveFreeAmount
	if free <= 0 {
		t.Fatalf("the network's free RC allowance is %d; the test needs the production 10,000", free)
	}
	if err := te.RcDb.SetRecord("hive:alice", 1, free); err != nil {
		t.Fatalf("seeding alice's frozen RC: %v", err)
	}
	if bal := te.SE.LedgerSystem.GetBalance("hive:alice", 1, "hbd"); bal != 0 {
		t.Fatalf("alice's HBD balance is %d; the test is about an account that never held any", bal)
	}
	if avail := te.SE.RcSystem.GetAvailableRCs("hive:alice", 1); avail != 0 {
		t.Fatalf("alice has %d RC available with her whole allowance frozen, want 0", avail)
	}
	return te, free
}

func TestRcTreadmill_RetryingWithNoRcLeavesTheReturnScheduleRunning(t *testing.T) {
	te, free := rcTreadmillEnv(t, 18) // 0.18.0 active

	const retries = 8
	for i := 0; i < retries; i++ {
		rcTreadmillRetry(te, fmt.Sprintf("rctm-new-%d", i))
	}

	// Not one of the retries was charged, so not one of them wrote her record:
	// the anchor she started on is the anchor she is still on.
	recs := rcRecords(te, "hive:alice")
	if len(recs) != 1 || recs[0].BlockHeight != 1 || recs[0].Amount != free {
		t.Fatalf("after %d retries alice's RC records are %+v; want only the seeded {height 1, amount %d} — "+
			"a retry she was not charged for must not re-anchor her return", retries, recs, free)
	}

	// And her RC comes back on the schedule it was already on.
	head := te.Reader.LastBlock
	if head <= 15 {
		t.Fatalf("only %d blocks passed; the test needs enough for the return to show", head)
	}
	avail := te.SE.RcSystem.GetAvailableRCs("hive:alice", head)
	if avail <= 0 {
		t.Fatalf("alice has %d RC available at block %d after %d retries; her free allowance must return "+
			"while she is out of RC", avail, head, retries)
	}
	wantAtHead := free - (free - free*int64(head-1)/144_000)
	if avail != wantAtHead {
		t.Fatalf("alice has %d RC at block %d, want %d (the unbroken return from her height-1 anchor)",
			avail, head, wantAtHead)
	}
}

func TestRcTreadmill_BelowTheLineEveryRetryReAnchorsHerReturn(t *testing.T) {
	te, free := rcTreadmillEnv(t, 17) // 0.17.0: before the line

	const retries = 8
	for i := 0; i < retries; i++ {
		rcTreadmillRetry(te, fmt.Sprintf("rctm-old-%d", i))
	}

	// The old rule: each retry is charged 100 RC alice does not have, the clamp
	// pins the total at her ceiling, and the record is re-stamped at the slot.
	recs := rcRecords(te, "hive:alice")
	if len(recs) < 2 {
		t.Fatalf("below 0.18.0 every retry must still write alice's record; got %+v — "+
			"settled history would re-execute differently", recs)
	}
	last := recs[len(recs)-1]
	if last.BlockHeight <= 1 {
		t.Fatalf("below the line the record must be re-anchored past height 1; got %+v", last)
	}
	if last.Amount != free {
		t.Fatalf("below the line the clamp pins the frozen total at the %d ceiling; got %d", free, last.Amount)
	}

	// Which is the bug: at the re-anchored height she has nothing, and every
	// further retry lands before the truncating divide returns a single RC.
	if avail := te.SE.RcSystem.GetAvailableRCs("hive:alice", last.BlockHeight); avail != 0 {
		t.Fatalf("below the line alice should have 0 RC at her re-anchored height %d; got %d",
			last.BlockHeight, avail)
	}
}
