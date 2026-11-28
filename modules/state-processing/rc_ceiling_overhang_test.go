package state_engine_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// RC-CEILING-OVERHANG-1 through the engine: an account whose RC record stores
// more than it can owe. It gets there by moving its HBD out — the record is
// clamped against its balance when a slot writes it, and the balance can fall
// afterwards — and it cannot get out, because the clamp runs only for an
// account a slot charged and a charge needs RC it no longer has.
//
// The record is seeded over the ceiling directly rather than produced by a
// transfer: what the rule is about is an account in that state, and the devnet
// cases cover a real transfer putting it there.

// rcOverhangEnv is alice holding a little HBD with an RC record far above what
// that balance can back, as if she had spent a large balance's worth of RC and
// then moved the balance away. Returns her ceiling.
func rcOverhangEnv(t *testing.T, consensus uint64) (*testEnv, int64) {
	t.Helper()
	te := newTestEnv()
	te.ElectionDb.ElectionsByHeight[1] = versionedElection(1, 1, 0, consensus)
	te.Creator.Transfer("alice", "vsc.gateway", "100000", "HBD", "")
	te.processAndWait()

	h := te.Reader.LastBlock
	bal := te.SE.LedgerSystem.GetBalance("hive:alice", h, "hbd")
	require.Greater(t, bal, int64(0), "alice must hold something, so her ceiling is not just the allowance")
	ceiling := bal + te.SE.RcSystem.HiveFreeAmount

	// Five times what she can owe: below the line that whole total has to decay
	// under her ceiling before she has a single RC, which is 4/5 of the return
	// period.
	require.NoError(t, te.RcDb.SetRecord("hive:alice", h, ceiling*5))
	require.Equal(t, int64(0), te.SE.RcSystem.GetAvailableRCs("hive:alice", h),
		"an account over its ceiling has nothing available the moment it gets there")
	return te, ceiling
}

// How much RC the account has after n blocks, and the figure an account owing
// exactly its ceiling would have at the same point.
func rcOverhangAfter(t *testing.T, te *testEnv, blocks int) uint64 {
	t.Helper()
	for mined := 0; mined < blocks; mined += 50 {
		te.Reader.MineNullBlocks(50)
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	return te.Reader.LastBlock
}

func TestRcCeilingOverhang_RecoversAtTheRateItsCeilingAllows(t *testing.T) {
	te, ceiling := rcOverhangEnv(t, 18) // 0.18.0 active
	start := te.Reader.LastBlock

	head := rcOverhangAfter(t, te, 200)
	elapsed := head - start
	avail := te.SE.RcSystem.GetAvailableRCs("hive:alice", head)

	// Capped at her ceiling, the return is ceiling * elapsed / 144,000.
	want := ceiling * int64(elapsed) / 144_000
	require.Equal(t, want, avail,
		"over %d blocks alice regained %d RC; capped at her %d ceiling the return is %d. An account over "+
			"its ceiling must come back at the rate that ceiling allows, not wait for the whole stored total "+
			"to decay under it", elapsed, avail, ceiling, want)
	require.Greater(t, avail, int64(0), "and it must actually be something she can see")
	t.Logf("ceiling %d, record %d: %d RC back over %d blocks", ceiling, ceiling*5, avail, elapsed)
}

func TestRcCeilingOverhang_BelowTheLineNothingComesBack(t *testing.T) {
	te, ceiling := rcOverhangEnv(t, 17) // 0.17.0: before the line
	start := te.Reader.LastBlock

	head := rcOverhangAfter(t, te, 200)
	elapsed := head - start
	avail := te.SE.RcSystem.GetAvailableRCs("hive:alice", head)

	require.Equal(t, int64(0), avail,
		"below the line alice must still have 0 RC after %d blocks: the return is a fraction of the stored "+
			"%d, and it has to fall under her %d ceiling before any of it is hers. Settled history depends on it",
		elapsed, ceiling*5, ceiling)
	t.Logf("below the line: still %d RC after %d blocks (the fix gives her %d)",
		avail, elapsed, ceiling*int64(elapsed)/144_000)
}

// The control: the cap must not hand RC to an account that is within its
// ceiling, which is every account that has not moved its balance. Same env, a
// record at exactly the ceiling instead of over it — the figure must be the one
// the old rule gives.
func TestRcCeilingOverhang_AnAccountWithinItsCeilingIsUnchanged(t *testing.T) {
	for _, consensus := range []uint64{17, 18} {
		te := newTestEnv()
		te.ElectionDb.ElectionsByHeight[1] = versionedElection(1, 1, 0, consensus)
		te.Creator.Transfer("alice", "vsc.gateway", "100000", "HBD", "")
		te.processAndWait()
		h := te.Reader.LastBlock
		ceiling := te.SE.LedgerSystem.GetBalance("hive:alice", h, "hbd") + te.SE.RcSystem.HiveFreeAmount
		require.NoError(t, te.RcDb.SetRecord("hive:alice", h, ceiling))

		head := rcOverhangAfter(t, te, 200)
		elapsed := head - h
		avail := te.SE.RcSystem.GetAvailableRCs("hive:alice", head)
		want := ceiling * int64(elapsed) / 144_000
		require.Equal(t, want, avail,
			"on consensus 0.%d an account owing exactly its ceiling regained %d RC over %d blocks, want %d — "+
				"the cap must bind only on an account that owes more than it can owe", consensus, avail, elapsed, want)
	}
}
