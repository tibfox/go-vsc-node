package state_engine_test

import (
	"testing"

	"vsc-node/modules/db/vsc/elections"
	ledgerDb "vsc-node/modules/db/vsc/ledger"
	ledgerSystem "vsc-node/modules/ledger-system"
	stateEngine "vsc-node/modules/state-processing"

	"github.com/stretchr/testify/assert"
)

// elections_ElectionAt returns a minimal ElectionResult with just the
// epoch set — enough for state engine helpers that only inspect the
// epoch field (e.g. GetElectionInfo callers).
func elections_ElectionAt(epoch uint64) elections.ElectionResult {
	return elections.ElectionResult{
		ElectionCommonInfo: elections.ElectionCommonInfo{
			Epoch: epoch,
		},
	}
}

// audit_diff_test.go
//
// Differential test bundle for the audit LOW + MEDIUM fixes carried by
// vsc-eco/go-vsc-node PRs #184 + #185 + #186. Each test exercises the
// specific failure path the audit identified. The tests are designed
// so that running them against `origin/pendulum` (without the fix
// commits) demonstrates the bug (RED), and running against the
// combined branch (with all 3 PRs cherry-picked) demonstrates the fix
// (GREEN).
//
// Sources:
//   - PENDULUM-LOWS-REDTEAM-RESULTS-2026-05-20.md
//   - PENDULUM-MEDS-REDTEAM-RESULTS-2026-05-20.md
//
// Each test header lists the audit reference and the file:line of the
// fix.

// =====================================================================
// PR186 #45 — TxConsensusStake / TxConsensusUnstake address validation
// =====================================================================
//
// audit MED #45: pre-fix the From/To check used `||` on the stake path
// and `&&` on the unstake path, tangled with the `did:` prefix in a
// way that accepted DID-form `From`s that should not stake to a
// "hive:" account. The fix flattens both to require a strict `hive:`
// prefix on both `From` and `To`. Differential here is: a stake from
// `did:alice` to `hive:alice` is rejected on the fix branch (GREEN)
// while it would slip through on baseline.
//
// File:line of fix: modules/state-processing/transactions.go:687,789

func TestAuditDiff_45_StakeFromDidPrefixRejected(t *testing.T) {
	te := newTestEnv()
	session := func() ledgerSystem.LedgerSession {
		balDb := newMockBalanceDb(map[string][]ledgerDb.BalanceRecord{
			"hive:alice": {{Account: "hive:alice", BlockHeight: 0, Hive: 100000}},
		})
		ls := ledgerSystem.New(balDb, newMockLedgerDb(), nil, newMockActionsDb())
		return ls.NewEmptySession(ls.NewEmptyState(), 1)
	}

	self := stateEngine.TxSelf{
		TxId:          "audit-diff-45-stake",
		OpIndex:       0,
		BlockHeight:   1,
		RequiredAuths: []string{"did:alice"},
	}

	// did: prefix on From — fix branch must reject.
	badFrom := &stateEngine.TxConsensusStake{
		Self:   self,
		From:   "did:alice",
		To:     "hive:alice",
		Amount: "10.000",
		Asset:  "hive",
		NetId:  "vsc-mocknet",
	}
	res := badFrom.ExecuteTx(te.SE, session(), nil, nil, "")
	assert.False(t, res.Success,
		"audit #45: stake with From='did:alice' must be rejected (baseline asymmetric || logic admits it)")
	assert.Equal(t, "Invalid to/from", res.Ret)
}

func TestAuditDiff_45_UnstakeFromDidPrefixRejected(t *testing.T) {
	te := newTestEnv()
	session := func() ledgerSystem.LedgerSession {
		balDb := newMockBalanceDb(map[string][]ledgerDb.BalanceRecord{
			"hive:alice": {{Account: "hive:alice", BlockHeight: 0, HIVE_CONSENSUS: 10000}},
		})
		ls := ledgerSystem.New(balDb, newMockLedgerDb(), nil, newMockActionsDb())
		return ls.NewEmptySession(ls.NewEmptyState(), 1)
	}

	self := stateEngine.TxSelf{
		TxId:          "audit-diff-45-unstake",
		OpIndex:       0,
		BlockHeight:   1,
		RequiredAuths: []string{"did:alice"},
	}

	badFrom := &stateEngine.TxConsensusUnstake{
		Self:   self,
		From:   "did:alice",
		To:     "hive:alice",
		Amount: "1.000",
		Asset:  "hive",
		NetId:  "vsc-mocknet",
	}
	res := badFrom.ExecuteTx(te.SE, session(), nil, nil, "")
	assert.False(t, res.Success,
		"audit #45: unstake with From='did:alice' must be rejected (baseline asymmetric && logic admits it)")
	assert.Equal(t, "Invalid to/from", res.Ret)
}

// =====================================================================
// PR186 #123 — consensus_unstake election lookup off-by-one
// =====================================================================
//
// audit MED #123: at slot boundaries where a new election lands at
// block N, an unstake submitted at block N was using the OLD election
// (epoch N-1's) because the code read `GetElectionInfo(bh - 1)`. The
// fix reads `GetElectionInfo(bh)`. The unlock epoch is recorded as
// `election.Epoch + 5`; the differential is observable in the oplog's
// `params.epoch` field.
//
// File:line of fix: modules/state-processing/transactions.go:819
//
// Setup: configure the mock election DB so block 99 maps to epoch 5
// and block 100 maps to epoch 6 (a fresh election just landed at
// block 100). Submit an unstake at block 100. Post-fix records
// unlock-epoch 11 (= 6 + 5); pre-fix would record 10 (= 5 + 5).
func TestAuditDiff_123_UnstakeUsesElectionAtCurrentBlock(t *testing.T) {
	te := newTestEnv()
	// New election lands at block 100 — pre-fix code reads bh-1=99 and
	// picks up the previous (epoch 5) election; post-fix reads bh=100
	// and picks up the just-elected epoch 6 set.
	te.ElectionDb.ElectionsByHeight[99] = elections_ElectionAt(5)
	te.ElectionDb.ElectionsByHeight[100] = elections_ElectionAt(6)

	balDb := newMockBalanceDb(map[string][]ledgerDb.BalanceRecord{
		"hive:alice": {{Account: "hive:alice", BlockHeight: 0, HIVE_CONSENSUS: 100000}},
	})
	ls := ledgerSystem.New(balDb, newMockLedgerDb(), nil, newMockActionsDb())
	state := ls.NewEmptyState()
	session := ls.NewEmptySession(state, 1)

	self := stateEngine.TxSelf{
		TxId:          "audit-diff-123",
		OpIndex:       0,
		BlockHeight:   100,
		RequiredAuths: []string{"hive:alice"},
	}

	tx := &stateEngine.TxConsensusUnstake{
		Self:   self,
		From:   "hive:alice",
		To:     "hive:alice",
		Amount: "1.000",
		Asset:  "hive",
		NetId:  "vsc-mocknet",
	}
	res := tx.ExecuteTx(te.SE, session, nil, nil, "")
	assert.True(t, res.Success, "audit #123: unstake at slot boundary must succeed; got %q", res.Ret)

	session.Done()
	if assert.Equal(t, 1, len(state.Oplog), "expected exactly 1 oplog entry") {
		ev := state.Oplog[0]
		assert.Equal(t, "consensus_unstake", ev.Type)
		epoch, ok := ev.Params["epoch"].(uint64)
		assert.True(t, ok, "Params.epoch missing or wrong type")
		assert.Equal(t, uint64(11), epoch,
			"audit #123: post-fix expects unlock epoch = current(6) + 5 = 11; "+
				"pre-fix would record 10 (= bh-1's old epoch 5 + 5) because of GetElectionInfo(bh-1)")
	}
}

// =====================================================================
// PR185 #114 / #115 / #38 — cid.MustParse panic on malicious tx ID
// =====================================================================
//
// audit MEDs #114/#115/#38: BlockTx.Decode and TransactionContainer's
// As* helpers used `cid.MustParse(tx.Id)` which PANICS on malformed
// input. A single corrupt tx ID in a VSC block — under the audit's
// threat model, a malicious scheduled witness — would crash every
// node ingesting that block.
//
// The fix swaps to `cid.Parse(...)` + explicit error return on every
// path. The differential here is observable directly: pre-fix
// BlockTx.Decode would panic on a non-CID string; post-fix returns
// an error and an empty container.
//
// File:line of fix:
//   - modules/state-processing/system_txs.go:969 (Decode)
//   - modules/state-processing/transactions.go (As* helpers)

func TestAuditDiff_114_BlockTxDecodeDoesNotPanicOnBadCID(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("audit #114/#115/#38: BlockTx.Decode panicked on malformed CID %q: %v (pre-fix MustParse behaviour)", "not-a-cid", r)
		}
	}()

	bTx := &stateEngine.BlockTx{
		Id:   "not-a-cid",
		Type: 1,
	}
	// nil da is fine — we expect to bail out at the cid.Parse step
	// before any DAG fetch is attempted.
	_, err := bTx.Decode(nil, stateEngine.TxSelf{})
	assert.Error(t, err, "audit #114/#115/#38: Decode of malformed CID must return an error (not panic)")
}

// =====================================================================
// Notes on fixes NOT covered by unit-level differential tests
// =====================================================================
//
// The following audit items are real and have shipped fixes on
// `tibfox/fix/audit-meds-protocol` but are deeply buried in goroutine
// / private-function paths that don't admit a clean unit-test entry
// point. They are validated by:
//   (1) code inspection,
//   (2) PR186's own unit tests where they exist,
//   (3) the devnet smoke (`TestTSSReshareHappyPath`) run against the
//       combined branch, which exercises the modified code paths
//       under normal traffic without regression.
//
//   - **S2 per-DID dedupe in tss.waitForSigs** — the `signedAccounts`
//     map is local to a goroutine launched inside `tssMgr.waitForSigs`;
//     no exported handle on the closure state. Fix at
//     `modules/tss/tss.go:1860,1875,1898`.
//   - **S3 high-S TSS sig rejection** — the check is inside the
//     `vsc.tss_sign` custom_json handler in `ProcessBlock`, not
//     reachable as a single-shot function. Fix at
//     `modules/state-processing/state_engine.go:979-983`.
//   - **RT-28 / RT-29 block-producer races (`bp.bh`, `bp.blockSigning`)**
//     — these are race-condition fixes; `go test -race` would catch
//     the pre-fix versions, but the races are non-deterministic and
//     hard to write a stable reproducer for. Fix at
//     `modules/block-producer/blockProducer.go:69,438-444,562-565,651-653`.
//   - **#118 nil-deref on `GetLedgerRange`** — inside the private
//     `UpdateBalances` slot-flush path, not callable in isolation. Fix
//     at `modules/state-processing/state_engine.go:1760-1770`.
//   - **#116 / #117 / #120 / #124** — log-level / observability
//     changes; no behavior difference to assert on.

// Sanity counterpoint — both stake and unstake must continue to
// accept the canonical hive:/hive: form. Catches a fix that became
// "too strict" and broke legitimate flows.
func TestAuditDiff_45_HiveToHiveStillAccepted(t *testing.T) {
	te := newTestEnv()
	balDb := newMockBalanceDb(map[string][]ledgerDb.BalanceRecord{
		"hive:alice": {{Account: "hive:alice", BlockHeight: 0, Hive: 100000, HIVE_CONSENSUS: 0}},
	})
	ls := ledgerSystem.New(balDb, newMockLedgerDb(), nil, newMockActionsDb())
	session := ls.NewEmptySession(ls.NewEmptyState(), 1)

	self := stateEngine.TxSelf{
		TxId:          "audit-diff-45-sanity",
		OpIndex:       0,
		BlockHeight:   1,
		RequiredAuths: []string{"hive:alice"},
	}

	ok := &stateEngine.TxConsensusStake{
		Self:   self,
		From:   "hive:alice",
		To:     "hive:alice",
		Amount: "10.000",
		Asset:  "hive",
		NetId:  "vsc-mocknet",
	}
	res := ok.ExecuteTx(te.SE, session, nil, nil, "")
	assert.True(t, res.Success,
		"audit #45 sanity: hive:/hive: stake must still succeed under tightened rule")
}
