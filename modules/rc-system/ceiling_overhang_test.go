package rc_system

import "testing"

// RC-CEILING-OVERHANG-1: an account's frozen RC is one absolute amount stored
// at one height, while the most it can owe — its ceiling — is its HBD balance
// plus the free allowance. Move the balance out and the stored amount is left
// far above the ceiling; since the return is a fraction of the STORED amount,
// nothing at all comes back until the whole of it has decayed under the
// ceiling. The account cannot be re-clamped out of it either: the clamp runs
// for an account a slot charged, and a charge needs RC it no longer has.
//
// From 0.18.0 the return is applied to the stored amount capped at the ceiling,
// so the account owes at most what it could owe and recovers in proportion.

const (
	period = 144_000 // RC_RETURN_PERIOD
	free   = 10_000  // RC_HIVE_FREE_AMOUNT
)

// available is what an account can spend: its ceiling less what it still owes.
func available(recordAmount, ceiling int64, elapsed uint64, capped bool) int64 {
	frozen := EffectiveFrozenBal(1, recordAmount, ceiling, 1+elapsed, capped)
	if frozen > ceiling {
		frozen = ceiling
	}
	return ceiling - frozen
}

// An account that spent 1,000 HBD worth of RC and then moved the HBD out, so
// all it has left is the free allowance.
func TestCeilingOverhang_AnAccountThatMovedItsBalanceOut(t *testing.T) {
	const spent = 1_010_000 // frozen against a 1,000 HBD balance
	const ceiling = free    // what it is left with

	// Below the line: the whole 1,010,000 has to decay under 10,000 first.
	// That is 98.9% of a five-day period — five days of no RC at all.
	for _, elapsed := range []uint64{15, 720, 1_440, 14_400, 100_000} {
		if got := available(spent, ceiling, elapsed, false); got != 0 {
			t.Errorf("below the line, %d blocks after moving the balance out: %d RC available, want 0 — "+
				"this is the bug, and the test is meant to show it", elapsed, got)
		}
	}
	if got := available(spent, ceiling, 142_600, false); got <= 0 {
		t.Errorf("below the line the account is still at %d RC after 142,600 blocks; it should finally be "+
			"coming back near the end of the period", got)
	}

	// On the line: it owes at most the 10,000 it could owe, and gets it back at
	// the free tier's rate — one RC every 15 blocks(ish), 100 RC in a day's
	// worth of blocks rather than five days of nothing.
	cases := []struct {
		elapsed uint64
		want    int64
	}{
		{14, 0},   // the truncating divide, as everywhere else
		{15, 1},   // 10,000 * 15 / 144,000
		{720, 50}, // half an hour of blocks
		{1_440, 100},
		{144_000, 10_000}, // the whole allowance back
	}
	for _, tc := range cases {
		if got := available(spent, ceiling, tc.elapsed, true); got != tc.want {
			t.Errorf("on the line, %d blocks after moving the balance out: %d RC available, want %d",
				tc.elapsed, got, tc.want)
		}
	}
}

// The cap changes nothing for an account that is not over its ceiling, which is
// every account that has not moved its balance. If this fails the rule is not a
// cap, it is a rewrite of the return.
func TestCeilingOverhang_NoOpForAnAccountWithinItsCeiling(t *testing.T) {
	const ceiling = 1_010_000
	for _, amount := range []int64{0, 1, 100, 500_000, ceiling} {
		for _, elapsed := range []uint64{0, 15, 1_440, 72_000, 144_000, 200_000} {
			capped := EffectiveFrozenBal(1, amount, ceiling, 1+elapsed, true)
			plain := EffectiveFrozenBal(1, amount, ceiling, 1+elapsed, false)
			if capped != plain {
				t.Fatalf("frozen %d of a %d ceiling after %d blocks: capped %d, uncapped %d — the cap must "+
					"only bind on an account that owes more than it can owe", amount, ceiling, elapsed, capped, plain)
			}
		}
	}
}

// The cap is a view of the record, not a rewrite of it: putting the balance
// back brings the obligation back on the schedule it would have had. Otherwise
// moving a balance out and back would wash the frozen total away.
func TestCeilingOverhang_PuttingTheBalanceBackRestoresTheDebt(t *testing.T) {
	const spent = 1_010_000
	const full = 1_010_000 // ceiling with the balance in place
	const elapsed = 1_440

	away := available(spent, free, elapsed, true)   // while the balance is gone
	back := available(spent, full, elapsed, true)   // with it put back
	plain := available(spent, full, elapsed, false) // what the old rule gave

	// The record was capped, not rewritten: with the balance back the figure is
	// the one the old rule gave, 1,010,000 * 1,440 / 144,000.
	if back != plain || back != 10_100 {
		t.Fatalf("with the balance back the account has %d RC; the old rule gave %d and the arithmetic says "+
			"10,100 — the cap must be a view of the record, not a rewrite of it", back, plain)
	}
	// And there is nothing to gain by the trip: away from its balance the
	// account had strictly less, not more.
	if away >= back {
		t.Fatalf("an account away from its balance had %d RC and with it back %d — if moving a balance out "+
			"paid better than leaving it, the cap would be a way to wash a frozen total away", away, back)
	}
}

// A ceiling of nothing (a non-Hive account with no HBD) is not a negative debt.
func TestCeilingOverhang_ZeroAndNegativeCeilings(t *testing.T) {
	if got := EffectiveFrozenBal(1, 5_000, 0, 1+1_000, true); got != 0 {
		t.Errorf("an account that can owe nothing is frozen %d, want 0", got)
	}
	if got := EffectiveFrozenBal(1, 5_000, -42, 1+1_000, true); got != 0 {
		t.Errorf("a negative ceiling is frozen %d, want 0 — a balance that has gone negative is not a credit", got)
	}
}
