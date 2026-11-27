package state_engine

import "testing"

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
