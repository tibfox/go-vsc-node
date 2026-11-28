package state_engine

import (
	"testing"

	rcSystem "vsc-node/modules/rc-system"
)

// What an account is charged when the charge is more than it has.
//
// Up to consensus 0.17.0 every failure path charged its flat RC whatever the
// payer held, and UpdateRcMap discarded the excess while re-stamping the
// payer's RC record. From 0.18.0 (RC-TREADMILL-1) a payer that cannot cover the
// charge is not charged at all — not even the part it could cover, which would
// take the RC it had just earned back and re-anchor its whole return.
func TestRcChargeIfAffordable(t *testing.T) {
	cases := []struct {
		name                     string
		available, already, want int64
		expect                   int64
	}{
		{"affordable, nothing charged yet", 10_000, 0, 100, 100},
		{"affordable, after an earlier charge in the slot", 10_000, 9_000, 100, 100},
		{"exactly what is left", 10_000, 9_900, 100, 100},
		{"one RC short: not charged at all", 10_000, 9_901, 100, 0},
		{"the trickle an out-of-RC account earned is left alone", 10_000, 9_999, 100, 0},
		{"nothing left", 10_000, 10_000, 100, 0},
		{"the slot already overcharged it", 10_000, 10_500, 100, 0},
		{"no RC at all (free allowance spent, no HBD)", 0, 0, 100, 0},
		{"the 50 RC charge of an undecodable tx", 0, 0, 50, 0},
		{"a charge of nothing stays nothing", 10_000, 0, 0, 0},
		{"a negative charge is never money back", 10_000, 0, -100, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RcChargeIfAffordable(tc.available, tc.already, tc.want)
			if got != tc.expect {
				t.Fatalf("RcChargeIfAffordable(available=%d, already=%d, want=%d) = %d, want %d",
					tc.available, tc.already, tc.want, got, tc.expect)
			}
			if got != 0 && got != tc.want {
				t.Fatalf("charged %d: a transaction is charged all of its %d RC or none of it", got, tc.want)
			}
			if got < 0 {
				t.Fatalf("charged a negative %d", got)
			}
		})
	}
}

// fixedRcSession is an RcSession that reports one total for every account, the
// way the real one reports balance + the free allowance less what is frozen.
// Consume is never called on the engine's execution path, so it is not needed
// here: what a slot has charged lives in se.RcMap, which is the whole point of
// the slot-aware gate.
type fixedRcSession struct {
	total int64
}

func (f fixedRcSession) CanConsume(account string, blockHeight uint64, rcAmt int64) (bool, int64, int64) {
	if f.total < rcAmt {
		return false, 0, rcAmt
	}
	return true, f.total - rcAmt, rcAmt
}
func (f fixedRcSession) Consume(account string, blockHeight uint64, rcAmt int64) (bool, int64) {
	panic("the execution path does not consume into the session")
}
func (f fixedRcSession) GetFrozenAmt(account string, blockHeight uint64) int64 { return 0 }
func (f fixedRcSession) HiveFreeAmount() int64                                 { return 10_000 }
func (f fixedRcSession) Revert()                                               {}
func (f fixedRcSession) Done() rcSystem.RcMapResult                            { return rcSystem.RcMapResult{} }

// RC-SLOT-GATE-1: what a slot has already charged a payer counts against what
// the payer may still spend in it. The RC a transaction is charged only reaches
// the payer's RC record when the slot closes, so an RcSession cannot see it and
// a gate that does not subtract it hands every transaction of the slot the same
// budget.
func TestRcAffordableCountsWhatTheSlotHasCharged(t *testing.T) {
	cases := []struct {
		name    string
		total   int64
		charged int64
		want    int64
		ok      bool
	}{
		{"nothing charged yet, affordable", 10_000, 0, 100, true},
		{"the slot has taken most of it, still affordable", 10_000, 9_900, 100, true},
		{"one RC short of a second charge", 199, 100, 100, false},
		{"exactly enough for a second charge", 200, 100, 100, true},
		{"the slot has taken all of it", 10_000, 10_000, 100, false},
		{"the slot has overrun what the payer holds", 10_000, 10_500, 50, false},
		{"an account with nothing", 0, 0, 50, false},
		{"a free call is affordable to anyone", 0, 0, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			se := &StateEngine{RcMap: map[string]int64{"hive:alice": tc.charged}}
			if got := se.RcAffordable(fixedRcSession{total: tc.total}, "hive:alice", 1, tc.want); got != tc.ok {
				t.Errorf("RcAffordable(total %d, slot has charged %d, wants %d) = %v, want %v",
					tc.total, tc.charged, tc.want, got, tc.ok)
			}
		})
	}
}

// The gate does not refuse what it cannot price: with no payer or no RC session
// there is nothing to read an account's RC from, and ChargeRc charges nothing
// in the same case, so the operation runs exactly as it did below the line.
// Refusing instead would turn an unwired RC system into a chain that rejects
// every operation.
func TestRcAffordableFailsOpenWithNothingToPriceWith(t *testing.T) {
	se := &StateEngine{RcMap: map[string]int64{}}
	if !se.RcAffordable(nil, "hive:alice", 1, 100) {
		t.Error("no RC session: the gate must not refuse")
	}
	if !se.RcAffordable(fixedRcSession{total: 0}, "", 1, 100) {
		t.Error("no payer: the gate must not refuse")
	}
}

// What a refusal reports: the RC the payer has left in this slot, never a debt.
func TestRcRemainingInSlot(t *testing.T) {
	se := &StateEngine{RcMap: map[string]int64{"hive:alice": 9_950, "hive:bob": 10_400}}
	if got := se.RcRemainingInSlot(fixedRcSession{total: 10_000}, "hive:alice", 1); got != 50 {
		t.Errorf("alice has %d RC left, want 50", got)
	}
	if got := se.RcRemainingInSlot(fixedRcSession{total: 10_000}, "hive:bob", 1); got != 0 {
		t.Errorf("bob's slot charges overran what he holds: %d RC left, want 0 and not a negative", got)
	}
	if got := se.RcRemainingInSlot(fixedRcSession{total: 10_000}, "hive:carol", 1); got != 10_000 {
		t.Errorf("carol has not been charged in this slot: %d RC left, want 10000", got)
	}
}

// RC-UNGATED-OPS-1: the gate prices an operation at what it is charged once it
// reaches the ledger — the success-path cost, which is also what a ledger
// refusal is charged. Pricing it at the cheaper validation-failure cost (50)
// would let a payer buy a success it cannot pay for.
func TestOpRcCostIsTheSuccessPathCost(t *testing.T) {
	want := map[string]int64{
		"transfer":          100,
		"withdraw":          200,
		"stake_hbd":         200,
		"unstake_hbd":       200,
		"consensus_stake":   100,
		"consensus_unstake": 100,
	}
	for op, w := range want {
		if got := OpRcCost(op); got != w {
			t.Errorf("OpRcCost(%q) = %d, want %d — the figure its ExecuteTx returns once it reaches the ledger", op, got, w)
		}
	}
	// An operation type nobody has priced is gated at the floor an undecodable
	// transaction is charged, not left free.
	if got := OpRcCost("some_op_added_later"); got != 50 {
		t.Errorf("an unpriced op type is gated at %d, want the 50 floor", got)
	}
	// A call is not in the table: its own gate prices it, because the same
	// figure is its gas budget.
	if OpRcCost("call") != 50 {
		t.Error("call must fall through to the floor — ExecuteBatch does not gate calls")
	}
}
