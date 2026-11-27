package rc_system_test

import (
	"testing"
	"vsc-node/lib/test_utils"
	"vsc-node/modules/common/params"
	rc_system "vsc-node/modules/rc-system"
)

// The return a Hive account with no HBD is on, and what re-anchoring it costs.
// Its whole budget is the free allowance, so its frozen total is the smallest
// the chain ever returns — and the one the truncating divide can stall.

func TestFreeTierRcReturnsOverTheReturnPeriod(t *testing.T) {
	const acct = "hive:bob"
	free := params.RC_HIVE_FREE_AMOUNT
	db := test_utils.NewMockRcDb()
	ls := &mockLedgerSystem{balances: map[string]int64{acct + ":hbd": 0}}
	rcs := rc_system.New(db, ls, free)
	db.SetRecord(acct, 1_000, free) // the whole allowance spent, anchored at 1000

	// free / RC_RETURN_PERIOD is 0.069 RC a block: the first whole RC takes 15
	// blocks, 50 RC (MINIMUM_RC_LIMIT, one cheapest transaction) takes 720, and
	// the allowance is whole again after the full period.
	cases := []struct {
		elapsed uint64
		want    int64
	}{
		{0, 0},
		{14, 0},
		{15, 1},
		{720, 50},
		{params.RC_RETURN_PERIOD / 10, free / 10},
		{params.RC_RETURN_PERIOD, free},
		{params.RC_RETURN_PERIOD * 2, free}, // never more than the allowance
	}
	for _, tc := range cases {
		got := rcs.GetAvailableRCs(acct, 1_000+tc.elapsed)
		if got != tc.want {
			t.Errorf("%d blocks after the anchor: %d RC available, want %d", tc.elapsed, got, tc.want)
		}
	}
}

// RC-TREADMILL-1, the half that is not the overcharge: re-anchoring the record
// at the closing slot restarts the return for the whole frozen total, and when
// (total * elapsed / RC_RETURN_PERIOD) truncates to zero the re-anchor throws
// away everything the account would have regained. At the free allowance that
// is any re-anchor under 15 blocks apart — one slot is 10 — which is why a slot
// that charges the account nothing must not write its record.
func TestReAnchoringTheFrozenTotalStallsTheReturn(t *testing.T) {
	free := params.RC_HIVE_FREE_AMOUNT
	period := params.RC_RETURN_PERIOD

	// Re-anchored every slot (10 blocks) for a full return period.
	frozen, anchor := free, uint64(1_000)
	for h := anchor + 10; h <= 1_000+period; h += 10 {
		frozen = rc_system.CalculateFrozenBal(anchor, h, frozen)
		anchor = h
	}
	if frozen != free {
		t.Fatalf("re-anchored every 10 blocks for a whole return period, frozen is %d; the point of "+
			"the test is that it is still the full %d — not one RC came back", frozen, free)
	}

	// Left alone over the same span, it is fully returned.
	if left := rc_system.CalculateFrozenBal(1_000, 1_000+period, free); left != 0 {
		t.Fatalf("left alone for a whole return period, frozen is %d, want 0", left)
	}

	// Even a slow re-anchor loses to it: the return is geometric, not linear.
	frozen, anchor = free, 1_000
	for h := anchor + 1_200; h <= 1_000+period; h += 1_200 { // once an hour
		frozen = rc_system.CalculateFrozenBal(anchor, h, frozen)
		anchor = h
	}
	if frozen <= 0 {
		t.Fatalf("hourly re-anchor over a whole return period left %d frozen; expected a remainder "+
			"the linear schedule would have returned", frozen)
	}
	t.Logf("after one return period: untouched 0 frozen, re-anchored hourly %d still frozen of %d",
		frozen, free)
}
