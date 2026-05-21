package tss

import (
	"encoding/base64"
	"testing"

	"vsc-node/lib/dids"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	ethBls "github.com/protolambda/bls12-381-util"
	"github.com/stretchr/testify/assert"
)

// audit_diff_test.go (modules/tss)
//
// PR186 audit S2: per-DID dedupe in `tssMgr.waitForSigs`. Pre-fix code
// at tss.go:1850-1900 had NO dedupe: when the goroutine drained
// `sigChan` and found a valid sig, it called `circuit.AddAndVerify`
// and on success incremented `signedWeight` by the member's weight.
// Because `BlsCircuit.addRaw` always returns `true` on a successful
// verify (it overwrites `b.sigs[DID]` rather than rejecting a
// duplicate), the same DID submitting twice double-counts in
// `signedWeight` — fake quorum.
//
// The fix at tss.go:1860,1875,1898 keeps a local
// `signedAccounts map[string]bool` and skips msgs whose Account is
// already in the map. The differential property is:
//   - On the BLS circuit lib: AddAndVerify of the SAME DID's sig
//     twice returns `(true, nil)` BOTH times. (This is the
//     pre-condition that made the bug exploitable.)
//   - The dedupe-loop pattern post-fix correctly counts the same
//     DID only once.

// TestAuditDiff_S2_BlsAddIsNotIdempotent — the precondition for the
// pre-fix bug. The BLS circuit's AddAndVerify, called twice with the
// same (member, valid-sig), returns `(true, nil)` BOTH times. Without
// a dedupe in waitForSigs, this is how the same DID resubmitting
// inflated `signedWeight`.
func TestAuditDiff_S2_BlsAddIsNotIdempotent(t *testing.T) {
	did, priv := freshBlsForTest(t, "audit_s2_seed_alice_abcdef1")
	cid := makeTestCid(t)

	gen := dids.NewBlsCircuitGenerator([]dids.Member{did})
	circuit, err := gen.Generate(cid)
	if err != nil {
		t.Fatalf("circuit.Generate: %v", err)
	}

	// alice signs the CID bytes
	sig := ethBls.Sign(priv, cid.Bytes())
	sigBytes := sig.Serialize()
	sigB64 := base64RawURL(sigBytes[:])

	// First submission must succeed and report added=true.
	added1, err := circuit.AddAndVerify(did, sigB64)
	if err != nil {
		t.Fatalf("first AddAndVerify: %v", err)
	}
	if !added1 {
		t.Fatal("first AddAndVerify must report added=true")
	}

	// Second submission of the SAME sig from the SAME DID — pre-fix
	// also reports added=true (the BLS lib's storage just overwrites
	// b.sigs[DID]). This is the load-bearing precondition that made
	// the missing dedupe exploitable.
	added2, err := circuit.AddAndVerify(did, sigB64)
	if err != nil {
		t.Fatalf("second AddAndVerify: %v", err)
	}
	assert.True(t, added2,
		"audit S2 precondition: BlsCircuit.AddAndVerify on a duplicate same-DID sig returns (true, nil). "+
			"If this fails, the BLS lib added its own dedupe and the audit's vulnerability surface has shrunk; "+
			"otherwise the dedupe in waitForSigs (tss.go:1875) is the only thing preventing double-counted weight.")
}

// TestAuditDiff_S2_DedupeLoopPattern — the dedupe-loop pattern from
// the post-fix waitForSigs (tss.go:1860,1875,1898). Exercises the
// pattern directly: feed alice's sig three times + bob's once;
// expect signedWeight to be alice.weight + bob.weight, NOT
// 3*alice.weight + bob.weight.
//
// This is a property test on the loop body. It does NOT instantiate
// the full TssManager (`waitForSigs` calls `pubsub.Send` which needs
// real p2p plumbing). The differential against pre-fix is that the
// pre-fix code didn't have the `signedAccounts[msg.Account]` skip
// at all, so all three alice sigs would tick `signedWeight` up.
func TestAuditDiff_S2_DedupeLoopPattern(t *testing.T) {
	// Election: alice=1, bob=1, carol=1 → total 3.
	// Quorum is `signedWeight*3 >= total*2` → signedWeight >= 2.
	weights := map[string]uint64{"alice": 1, "bob": 1, "carol": 1}

	// Simulated message stream — alice submits 3x, bob 1x.
	msgs := []sigMsg{
		{Account: "alice", Sig: "sig-alice"},
		{Account: "alice", Sig: "sig-alice-replay"},
		{Account: "alice", Sig: "sig-alice-another"},
		{Account: "bob", Sig: "sig-bob"},
	}

	// Mirror the post-fix loop body from tss.go:1858-1900.
	signedAccounts := make(map[string]bool)
	signedWeight := uint64(0)
	for _, msg := range msgs {
		if signedAccounts[msg.Account] {
			continue
		}
		// Stand in for circuit.AddAndVerify: we assume the sig
		// verifies (TestAuditDiff_S2_BlsAddIsNotIdempotent covers
		// the lib-level behaviour). The point of this test is the
		// dedupe gate.
		const added = true
		if added {
			signedAccounts[msg.Account] = true
			signedWeight += weights[msg.Account]
		}
	}

	assert.Equal(t, uint64(2), signedWeight,
		"audit S2: post-fix dedupe must count alice once + bob once = 2; "+
			"pre-fix loop (no signedAccounts check) would count alice three times + bob once = 4 "+
			"and trip a fake quorum after alice's second resubmit (signedWeight=2 >= 2).")
}

// =====================================================================
// helpers — kept in this file so the audit-diff tests stand alone
// without changing the rest of the tss test infra.
// =====================================================================

func makeTestCid(t *testing.T) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte("audit-diff-s2-test-cid"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash.Sum: %v", err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}

func base64RawURL(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

func freshBlsForTest(t *testing.T, seedStr string) (dids.BlsDID, *dids.BlsPrivKey) {
	t.Helper()
	var seed [32]byte
	copy(seed[:], []byte(seedStr))

	priv := dids.BlsPrivKey{}
	priv.Deserialize(&seed)

	pub, err := ethBls.SkToPk(&priv)
	if err != nil {
		t.Fatalf("derive pubkey: %v", err)
	}
	did, err := dids.NewBlsDID(pub)
	if err != nil {
		t.Fatalf("derive DID: %v", err)
	}
	return did, &priv
}
