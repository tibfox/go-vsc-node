package gateway

import "math"

// review6 L4: defensive cap. The arithmetic `totalWeight*2 + 2` overflows
// signed int at totalWeight >= 2^62 (on 64-bit). Dormant under the 40-key
// practical cap, but a config change that lifts the cap would re-arm
// the overflow. Reject inputs that would overflow before the cap can be
// lifted; callers must already validate totalWeight upstream, this is the
// last line of defense.
const maxGatewayTotalWeight = (math.MaxInt - 2) / 2

// gatewayWeightThreshold returns the owner-auth weight threshold for the
// gateway multisig account: a strict 2/3 supermajority of totalWeight.
//
// review2 HIGH #29: this was previously int(totalWeight * 2 / 3), i.e.
// floor(2N/3). For 10 keys that yields 6, letting 6-of-10 signers move
// funds when a 2/3 supermajority should require 7. ceil(2N/3) is the correct
// threshold; ceil(a/b) == (a + b - 1) / b, so ceil(2N/3) == (2N + 2) / 3.
//
// review6 L4: panic on overflow-class totalWeight rather than silently
// returning a wrong threshold. The 40-key practical cap keeps real callers
// far below this; tripping it indicates a config bug or attacker-controlled
// totalWeight that should fail loudly.
func gatewayWeightThreshold(totalWeight int) int {
	if totalWeight <= 0 {
		return 0
	}
	if totalWeight > maxGatewayTotalWeight {
		panic("gatewayWeightThreshold: totalWeight exceeds safe range")
	}
	return (totalWeight*2 + 2) / 3
}
