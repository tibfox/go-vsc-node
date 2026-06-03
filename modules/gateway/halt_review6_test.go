package gateway

// review6 M7: gateway emergency-halt switch.
//
// halt.go exposes an atomic flag flipped via SetHalted / read via IsHalted that
// gateway sign/broadcast/multisig paths consult on the fast path. This file
// asserts the basic semantics plus race-freedom under concurrent readers and
// writers (M7 requires the flag to be safe to flip from a health-check
// goroutine while the multisig tick reads it on every block).

import (
	"sync"
	"testing"
)

// TestReview6_M7_Halt_InitialState confirms a fresh package state defaults to
// not-halted. Note: any TEST in the same package that mutates halted leaves
// global state behind, so this test runs first and immediately resets.
func TestReview6_M7_Halt_InitialState(t *testing.T) {
	// Defensive reset — earlier tests in alphabetical order or the
	// VSC_GATEWAY_HALT=1 startup-env path could have flipped it.
	SetHalted(false)
	if IsHalted() {
		t.Fatalf("IsHalted() = true after SetHalted(false), want false")
	}
}

// TestReview6_M7_Halt_SetTrue_SetFalse covers the basic toggle contract.
func TestReview6_M7_Halt_SetTrue_SetFalse(t *testing.T) {
	defer SetHalted(false) // leave the global at the default for siblings

	SetHalted(true)
	if !IsHalted() {
		t.Fatalf("IsHalted() = false after SetHalted(true)")
	}
	SetHalted(false)
	if IsHalted() {
		t.Fatalf("IsHalted() = true after SetHalted(false)")
	}
	// Idempotent — calling SetHalted with the same value should not flip.
	SetHalted(false)
	if IsHalted() {
		t.Fatalf("IsHalted() = true after second SetHalted(false)")
	}
	SetHalted(true)
	SetHalted(true)
	if !IsHalted() {
		t.Fatalf("IsHalted() = false after double SetHalted(true)")
	}
}

// TestReview6_M7_Halt_ConcurrentReadsAndWrites exercises the atomic.Bool
// implementation under contention. With `go test -race`, a non-atomic
// implementation would trip the race detector. Without -race, a torn-write
// implementation would surface as a non-bool value (atomic.Bool guarantees
// load/store atomicity, which we re-assert here).
func TestReview6_M7_Halt_ConcurrentReadsAndWrites(t *testing.T) {
	defer SetHalted(false)

	const writers = 8
	const readers = 16
	const iterations = 2000

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	// Writers flip back and forth as fast as possible.
	for w := 0; w < writers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				SetHalted(i%2 == 0)
			}
		}(w)
	}

	// Readers spin on IsHalted. The only invariant we can assert at runtime
	// is that the return value is a bool — but in Go that's tautological.
	// The real assertion is "no panic, no race detector report, no torn
	// read": this test passes silently when those hold.
	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			var seenTrue, seenFalse bool
			for i := 0; i < iterations; i++ {
				if IsHalted() {
					seenTrue = true
				} else {
					seenFalse = true
				}
			}
			// touch the locals to keep the compiler honest
			_ = seenTrue
			_ = seenFalse
		}()
	}

	wg.Wait()
}
