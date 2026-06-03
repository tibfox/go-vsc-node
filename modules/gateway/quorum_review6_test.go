package gateway

// review6 L4: gatewayWeightThreshold overflow guard.
//
// The arithmetic `totalWeight*2 + 2` overflows signed int at
// totalWeight >= (MaxInt-2)/2 (≈ 2^62 on 64-bit). The fix adds an explicit
// panic with a stable message — callers are expected to validate inputs
// upstream and this is the last line of defense.
//
// This file (a) reasserts the math for representative inputs so the panic
// guard doesn't regress the happy path, and (b) drives MaxInt straight into
// the function and catches the panic via recover().

import (
	"math"
	"strings"
	"testing"
)

// TestReview6_L4_GatewayWeightThreshold_NormalInputs covers the documented
// representative cases — including the edge inputs 1 and 2 that the prior
// floor(2N/3) implementation rounded down to zero/one.
func TestReview6_L4_GatewayWeightThreshold_NormalInputs(t *testing.T) {
	cases := []struct {
		total int
		want  int
	}{
		{0, 0},   // guard short-circuit
		{1, 1},   // ceil(2/3)
		{2, 2},   // ceil(4/3) — floor would give 1
		{3, 2},   // ceil(6/3) — exact 2
		{10, 7},  // user-cited
		{15, 10}, // ceil(30/3) = 10
		{19, 13}, // ceil(38/3) = 13
	}
	for _, c := range cases {
		if got := gatewayWeightThreshold(c.total); got != c.want {
			t.Errorf("gatewayWeightThreshold(%d) = %d, want %d", c.total, got, c.want)
		}
	}
}

// TestReview6_L4_GatewayWeightThreshold_OverflowPanics drives MaxInt through
// the function — without the L4 guard the arithmetic would silently wrap and
// return an incorrect threshold, allowing a misconfigured caller to set a
// trivially-passable owner-auth weight on the gateway multisig.
//
// The fix asserts a *named* panic message, which we capture via recover() and
// substring-match. A bare panic would still trip the test (any panic crashes
// the test runner), but the substring check ensures the guard is the one
// firing rather than a downstream allocation or array-index panic.
func TestReview6_L4_GatewayWeightThreshold_OverflowPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("gatewayWeightThreshold(MaxInt) did not panic — overflow guard missing")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panic value is %T, want string with overflow message", r)
		}
		if !strings.Contains(msg, "exceeds safe range") {
			t.Fatalf("panic message %q does not mention 'exceeds safe range'", msg)
		}
	}()

	_ = gatewayWeightThreshold(math.MaxInt)
}

// TestReview6_L4_GatewayWeightThreshold_BoundaryAcceptsMaxAllowed asserts the
// boundary just below the panic cap is still accepted — this proves the
// rejection is *only* on the unsafe path, not a too-aggressive guard that
// would reject legitimate (albeit large) totalWeight inputs.
func TestReview6_L4_GatewayWeightThreshold_BoundaryAcceptsMaxAllowed(t *testing.T) {
	// (MaxInt-2)/2 is the largest value for which (totalWeight*2 + 2)
	// arithmetic does not overflow. Calling with exactly this value must NOT
	// panic.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("gatewayWeightThreshold((MaxInt-2)/2) panicked: %v — guard too aggressive", r)
		}
	}()
	_ = gatewayWeightThreshold((math.MaxInt - 2) / 2)
}
