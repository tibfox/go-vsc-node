package gateway

import (
	"os"
	"sync/atomic"
)

// review6 M7: emergency-halt switch for gateway witnessing.
//
// Pre-fix, an operator who wanted to stop their node from participating in
// fund-moving multisig actions (key rotation, sign-response signing,
// withdrawal-action signing) had no mechanism short of killing the
// process. Killing is destructive: it drops libp2p sessions, churns
// consensus, and rules out a partial-participation posture (e.g. "still
// produce blocks, just don't sign withdrawals").
//
// This file adds two surfaces:
//
//  1. an atomic boolean flag exposed via SetHalted / IsHalted; any module
//     can flip it from in-process (e.g. health-check failure handler);
//
//  2. an environment-variable trigger (VSC_GATEWAY_HALT=1) checked once at
//     startup so operators can bring up a halted node without recompiling.
//
// Callers that touch sign / broadcast / multisig dispatch are expected to
// early-return when IsHalted() is true. The flag is read on the fast path
// per tick so changes propagate immediately (no restart required to
// resume).
//
// Halt is a SAFETY brake, not a security boundary: an attacker who has
// already taken over the process can flip it back on. It exists to give
// operators a graceful-degradation tool, e.g. after they spot an in-flight
// loss-of-funds incident and want to stop the node from co-signing
// anything further while the rest of the network sorts out the response.
var halted atomic.Bool

func init() {
	if os.Getenv("VSC_GATEWAY_HALT") == "1" {
		halted.Store(true)
	}
}

// IsHalted returns true if gateway witnessing is currently paused.
func IsHalted() bool { return halted.Load() }

// SetHalted toggles the gateway-witnessing halt state. Idempotent.
func SetHalted(v bool) { halted.Store(v) }
