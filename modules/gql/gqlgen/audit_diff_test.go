package gqlgen

import (
	"testing"
	"time"
)

// audit LOW (PR184) — `simulateContractCalls` had no auth + no rate
// limit. The PR added a per-process token bucket (default 30/min) to
// cap WASM execution caused by simulation traffic.
//
// File:line of fix:
//   - modules/gql/gqlgen/simulate_ratelimit.go (whole file, new)
//   - modules/gql/gqlgen/schema.resolvers.go:587 (Allow() gate added)
//
// Differential: pre-fix the bucket is absent; ANY number of consecutive
// Allow() calls would succeed. Post-fix the bucket caps at `max` (30
// by default at 1-minute refill interval). The test exercises the
// token bucket directly with a 1-second window to keep the test fast.

// TestAuditDiff_GQL_TokenBucketCapsAtMax — the bucket admits exactly
// `max` consecutive calls in a fresh window, then rejects until refill.
// On a pre-fix build the bucket doesn't exist at all, and the
// `Allow()` symbol isn't defined — the file simply won't compile,
// which is itself the differential signal.
func TestAuditDiff_GQL_TokenBucketCapsAtMax(t *testing.T) {
	tb := newTokenBucket(5, 10*time.Second)

	for i := 0; i < 5; i++ {
		if !tb.Allow() {
			t.Fatalf("audit GQL: bucket should admit first %d calls; rejected at call %d", 5, i+1)
		}
	}

	// The 6th call must be rejected — bucket is empty, no time has
	// elapsed for a refill in test setup.
	if tb.Allow() {
		t.Fatal("audit GQL: bucket should reject the 6th call; pre-fix had no bucket at all and would admit unlimited calls")
	}
}

// TestAuditDiff_GQL_TokenBucketRefills — after the configured
// interval passes, tokens replenish proportionally. Catches a fix
// where the bucket never refills (= 30/process/lifetime — too strict).
func TestAuditDiff_GQL_TokenBucketRefills(t *testing.T) {
	// 4 tokens, 200ms full-refill interval → 50ms per token.
	tb := newTokenBucket(4, 200*time.Millisecond)

	// Drain.
	for i := 0; i < 4; i++ {
		if !tb.Allow() {
			t.Fatalf("drain failed at call %d", i+1)
		}
	}
	if tb.Allow() {
		t.Fatal("drained bucket should reject")
	}

	// Wait for a full window — should refill all 4 tokens.
	time.Sleep(250 * time.Millisecond)

	allowed := 0
	for i := 0; i < 4; i++ {
		if tb.Allow() {
			allowed++
		}
	}
	if allowed < 3 {
		t.Fatalf("after refill window, expected ~4 admits, got %d (bucket didn't replenish)", allowed)
	}
}
