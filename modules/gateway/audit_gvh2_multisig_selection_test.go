package gateway

// review7 GV-H2 — the gateway multisig selected the 40 LOWEST-stake witnesses.
//
// keyRotation sorted the candidate gateway keys by ASCENDING weight and took
// the first 40, so the committee that signs BTC/HBD withdrawals was backed by
// the cheapest-to-acquire keys. It must instead select the HIGHEST-staked
// witnesses. This test pins selectTopWeightGatewayKeys to the top-by-weight
// set; it fails against the ascending order and passes once the order is
// descending (with a deterministic tie-break).

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func keyOf(k [2]interface{}) string { return k[0].(string) }

func TestAuditReview7_GVH2_SelectsHighestStakeKeys(t *testing.T) {
	weightMap := map[string]uint64{
		"k_low":   5,
		"k_mid":   30,
		"k_high":  100,
		"k_huge":  1 << 62, // exercises the uint64-direct comparison (no int truncation)
		"k_small": 1,
	}
	keys := make([][2]interface{}, 0, len(weightMap))
	for name := range weightMap {
		keys = append(keys, [2]interface{}{name, 1})
	}

	got := selectTopWeightGatewayKeys(keys, weightMap, 3)
	require.Len(t, got, 3)

	gotSet := map[string]bool{}
	for _, k := range got {
		gotSet[keyOf(k)] = true
	}
	require.True(t, gotSet["k_huge"], "must include the highest-stake key")
	require.True(t, gotSet["k_high"], "must include the 2nd highest-stake key")
	require.True(t, gotSet["k_mid"], "must include the 3rd highest-stake key")
	require.False(t, gotSet["k_low"], "must NOT include a low-stake key")
	require.False(t, gotSet["k_small"], "must NOT include the lowest-stake key")
}

func TestAuditReview7_GVH2_DeterministicTieBreak(t *testing.T) {
	// Equal weights at the cutoff boundary must resolve identically every run.
	weightMap := map[string]uint64{"a": 10, "b": 10, "c": 10, "d": 10}
	mk := func() [][2]interface{} {
		return [][2]interface{}{{"d", 1}, {"b", 1}, {"a", 1}, {"c", 1}}
	}
	first := selectTopWeightGatewayKeys(mk(), weightMap, 2)
	for i := 0; i < 5; i++ {
		again := selectTopWeightGatewayKeys(mk(), weightMap, 2)
		require.Equal(t, keyOf(first[0]), keyOf(again[0]))
		require.Equal(t, keyOf(first[1]), keyOf(again[1]))
	}
	// Tie-break is by key string ascending: {a,b}.
	require.Equal(t, "a", keyOf(first[0]))
	require.Equal(t, "b", keyOf(first[1]))
}
