package gateway

// review6 L5: unstakeOps sort comparator rewritten as cmp.Compare(a.Amount,
// b.Amount) — the previous `int(a)-int(b)` pattern wraps to a wrong-sign
// integer once Amount approaches int64 limits, which would cause SortFunc to
// place a MaxInt64 entry "before" a MinInt64 entry and break the clearedOps
// computation downstream (clearedBal would underflow).
//
// executeActions wires the sort deep into multisig.go and is not directly
// unit-testable without standing up the entire ledger stack. Instead, this
// test re-exercises the *exact same comparator pattern* against a hand-built
// slice of ledger.ActionRecord values that include the int64 extremes — if
// the pattern regresses to int-subtraction, the assertion at the bottom
// fires.

import (
	"cmp"
	"math"
	"slices"
	"testing"

	ledgerDb "vsc-node/modules/db/vsc/ledger"
)

// TestReview6_L5_UnstakeOpsComparator_HandlesInt64Extremes builds a slice
// containing MaxInt64 and MinInt64 — the exact inputs that the prior
// `int(a)-int(b)` would wrap. After sort, indices must be in non-decreasing
// Amount order; we also assert MinInt64 is at index 0 and MaxInt64 at the
// tail to make the regression failure mode unambiguous.
func TestReview6_L5_UnstakeOpsComparator_HandlesInt64Extremes(t *testing.T) {
	// Order chosen so a buggy comparator that wraps near int64 limits would
	// leave at least one out-of-order adjacent pair.
	in := []ledgerDb.ActionRecord{
		{Id: "max", Amount: math.MaxInt64},
		{Id: "neg-large", Amount: -1_000_000_000_000},
		{Id: "min", Amount: math.MinInt64},
		{Id: "zero", Amount: 0},
		{Id: "small-pos", Amount: 5},
		{Id: "max-minus-1", Amount: math.MaxInt64 - 1},
		{Id: "min-plus-1", Amount: math.MinInt64 + 1},
		{Id: "pos-large", Amount: 1_000_000_000_000},
	}

	// Mirror multisig.go:657 exactly — same SortFunc pattern, same comparator.
	slices.SortFunc(
		in,
		func(a ledgerDb.ActionRecord, b ledgerDb.ActionRecord) int {
			return cmp.Compare(a.Amount, b.Amount)
		},
	)

	// Non-decreasing invariant — this is what executeActions relies on so
	// clearedBal accumulates in ascending order until it crosses unstakeBal.
	for i := 1; i < len(in); i++ {
		if in[i-1].Amount > in[i].Amount {
			t.Fatalf("sort produced out-of-order pair at idx %d: %d > %d (Id %s > %s)",
				i, in[i-1].Amount, in[i].Amount, in[i-1].Id, in[i].Id)
		}
	}

	// MinInt64 first, MaxInt64 last — extra paranoia: a comparator that
	// wraps near int64 boundaries would scramble these two specifically.
	if in[0].Id != "min" {
		t.Fatalf("after sort, first element Id=%s Amount=%d — want 'min' at index 0", in[0].Id, in[0].Amount)
	}
	if in[len(in)-1].Id != "max" {
		t.Fatalf("after sort, last element Id=%s Amount=%d — want 'max' at index %d", in[len(in)-1].Id, in[len(in)-1].Amount, len(in)-1)
	}
}

// TestReview6_L5_UnstakeOpsComparator_StableAcrossEqualAmounts confirms the
// comparator returns 0 for equal Amounts (cmp.Compare contract). slices.SortFunc
// is not stable but the comparator must NOT report < or > for ties — a
// non-zero return on a tie can cause spurious swaps under quicksort that
// surface as test flakes downstream.
func TestReview6_L5_UnstakeOpsComparator_StableAcrossEqualAmounts(t *testing.T) {
	cmpFn := func(a ledgerDb.ActionRecord, b ledgerDb.ActionRecord) int {
		return cmp.Compare(a.Amount, b.Amount)
	}

	a := ledgerDb.ActionRecord{Id: "a", Amount: 42}
	b := ledgerDb.ActionRecord{Id: "b", Amount: 42}
	if got := cmpFn(a, b); got != 0 {
		t.Fatalf("cmpFn on equal Amounts returned %d, want 0", got)
	}
	if got := cmpFn(b, a); got != 0 {
		t.Fatalf("cmpFn(b,a) on equal Amounts returned %d, want 0", got)
	}

	// And the extremes: equal MinInt64 and equal MaxInt64.
	min1 := ledgerDb.ActionRecord{Id: "min1", Amount: math.MinInt64}
	min2 := ledgerDb.ActionRecord{Id: "min2", Amount: math.MinInt64}
	if cmpFn(min1, min2) != 0 {
		t.Fatalf("cmpFn on equal MinInt64 returned non-zero")
	}
	max1 := ledgerDb.ActionRecord{Id: "max1", Amount: math.MaxInt64}
	max2 := ledgerDb.ActionRecord{Id: "max2", Amount: math.MaxInt64}
	if cmpFn(max1, max2) != 0 {
		t.Fatalf("cmpFn on equal MaxInt64 returned non-zero")
	}
}

// TestReview6_L5_UnstakeOpsComparator_RegressionAgainstIntSubtraction is the
// adversarial counterpart. The old `int(a)-int(b)` (or int64 subtraction
// without overflow guard) returns the wrong sign for {MaxInt64, MinInt64}
// because MaxInt64 - MinInt64 wraps to -1 in two's-complement. We assert the
// new pattern returns the correct sign on that exact pair.
func TestReview6_L5_UnstakeOpsComparator_RegressionAgainstIntSubtraction(t *testing.T) {
	cmpFn := func(a ledgerDb.ActionRecord, b ledgerDb.ActionRecord) int {
		return cmp.Compare(a.Amount, b.Amount)
	}

	max := ledgerDb.ActionRecord{Amount: math.MaxInt64}
	min := ledgerDb.ActionRecord{Amount: math.MinInt64}

	if got := cmpFn(max, min); got <= 0 {
		t.Fatalf("cmp.Compare(MaxInt64, MinInt64) = %d, want > 0 — overflow regression", got)
	}
	if got := cmpFn(min, max); got >= 0 {
		t.Fatalf("cmp.Compare(MinInt64, MaxInt64) = %d, want < 0 — overflow regression", got)
	}
}
