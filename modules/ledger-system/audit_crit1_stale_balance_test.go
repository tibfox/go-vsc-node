package ledgerSystem_test

// review7 CRIT-1 — stale GetBalance filter enables a silent double-spend.
//
// LedgerState.GetBalance (the spend-check read at ledger_session.go:310/167/356)
// must equal what the authoritative reconciler StateEngine.UpdateBalances
// computes for the same (account, height, asset): the BalanceDb snapshot plus
// the net of every LedgerDb record past the snapshot height (skipping only the
// safety-slash meta rows). UpdateBalances sums ALL record types (empty OpType
// filter, state_engine.go:2011); GetBalance instead used a positives-only
// whitelist per asset:
//
//	hbd          -> {"unstake","deposit"}                 (drops transfer-/withdraw-/stake- debits)
//	hive         -> {"deposit", restitution}              (drops transfer-/withdraw-/consensus_stake- debits)
//	hbd_savings  -> {"stake"}                              (drops the unstake- debit)
//
// so any outgoing debit committed after the last snapshot was excluded from the
// spend-check balance — the read over-reports, and the gate at
// ledger_session.go:316 `(fromBal - exclusion) < amount` lets the same funds be
// spent again. These tests fail (over-reported balance / accepted second spend)
// against the pre-fix filters and pass once GetBalance mirrors UpdateBalances.

import (
	"testing"

	"vsc-node/lib/test_utils"
	ledgerDb "vsc-node/modules/db/vsc/ledger"
	ledgerSystem "vsc-node/modules/ledger-system"

	"github.com/stretchr/testify/require"
)

// seedLedger appends a raw ledger record for an account on the mock ledger DB.
func seedLedger(state *ledgerSystem.LedgerState, rec ledgerDb.LedgerRecord) {
	db := state.LedgerDb.(*test_utils.MockLedgerDb)
	_ = db.StoreLedger(rec)
}

// TestAuditReview7_CRIT1_StaleBalanceDoubleSpend drives a real double-spend
// through the production ExecuteTransfer spend gate: an account that deposited
// 100k HBD and already transferred all of it out (true balance 0) is allowed to
// transfer another 100k because GetBalance still reports the stale 100k.
func TestAuditReview7_CRIT1_StaleBalanceDoubleSpend(t *testing.T) {
	state := newTestState()
	const bh = uint64(100)

	// Committed history: +100k deposit, then -100k transfer out. Net = 0.
	seedLedger(state, ledgerDb.LedgerRecord{
		Id: "dep#out", Owner: "hive:alice", Amount: 100000, Asset: "hbd",
		Type: "deposit", BlockHeight: 10,
	})
	seedLedger(state, ledgerDb.LedgerRecord{
		Id: "xfer1#in", Owner: "hive:alice", Amount: -100000, Asset: "hbd",
		Type: "transfer", BlockHeight: 20,
	})

	// Authoritative balance is 0. This is the over-report at the heart of CRIT-1.
	require.Equal(t, int64(0), state.GetBalance("hive:alice", bh, "hbd"),
		"GetBalance must net out the outgoing transfer (true balance 0)")

	// The actual spend gate: a SECOND 100k transfer must be rejected.
	session := ledgerSystem.NewSession(state)
	defer session.Revert()
	res := session.ExecuteTransfer(ledgerSystem.OpLogEvent{
		Id: "xfer2", From: "hive:alice", To: "hive:bob",
		Amount: 100000, Asset: "hbd", BlockHeight: bh,
	})
	require.False(t, res.Ok,
		"second 100k transfer must be rejected — accepting it double-spends the same funds (msg=%q)", res.Msg)
	require.Equal(t, "insufficient balance", res.Msg)
}

// TestAuditReview7_CRIT1_GetBalanceMirrorsLedger asserts GetBalance nets every
// debit type across all three spendable assets, matching UpdateBalances.
func TestAuditReview7_CRIT1_GetBalanceMirrorsLedger(t *testing.T) {
	const bh = uint64(100)

	cases := []struct {
		name   string
		asset  string
		recs   []ledgerDb.LedgerRecord
		expect int64
	}{
		{
			name:  "hbd nets transfer-out debit",
			asset: "hbd",
			recs: []ledgerDb.LedgerRecord{
				{Id: "d", Amount: 100000, Asset: "hbd", Type: "deposit", BlockHeight: 10},
				{Id: "t", Amount: -100000, Asset: "hbd", Type: "transfer", BlockHeight: 20},
			},
			expect: 0,
		},
		{
			name:  "hbd nets withdraw debit",
			asset: "hbd",
			recs: []ledgerDb.LedgerRecord{
				{Id: "d", Amount: 100000, Asset: "hbd", Type: "deposit", BlockHeight: 10},
				{Id: "w", Amount: -40000, Asset: "hbd", Type: "withdraw", BlockHeight: 20},
			},
			expect: 60000,
		},
		{
			name:  "hbd nets stake debit",
			asset: "hbd",
			recs: []ledgerDb.LedgerRecord{
				{Id: "d", Amount: 100000, Asset: "hbd", Type: "deposit", BlockHeight: 10},
				{Id: "s", Amount: -30000, Asset: "hbd", Type: "stake", BlockHeight: 20},
			},
			expect: 70000,
		},
		{
			name:  "hive nets transfer-out debit",
			asset: "hive",
			recs: []ledgerDb.LedgerRecord{
				{Id: "d", Amount: 50000, Asset: "hive", Type: "deposit", BlockHeight: 10},
				{Id: "t", Amount: -50000, Asset: "hive", Type: "transfer", BlockHeight: 20},
			},
			expect: 0,
		},
		{
			name:  "hive nets consensus_stake debit",
			asset: "hive",
			recs: []ledgerDb.LedgerRecord{
				{Id: "d", Amount: 50000, Asset: "hive", Type: "deposit", BlockHeight: 10},
				{Id: "cs", Amount: -20000, Asset: "hive", Type: "consensus_stake", BlockHeight: 20},
			},
			expect: 30000,
		},
		{
			name:  "hbd_savings nets unstake debit",
			asset: "hbd_savings",
			recs: []ledgerDb.LedgerRecord{
				{Id: "s", Amount: 30000, Asset: "hbd_savings", Type: "stake", BlockHeight: 10},
				{Id: "u", Amount: -30000, Asset: "hbd_savings", Type: "unstake", BlockHeight: 20},
			},
			expect: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState()
			for _, r := range tc.recs {
				r.Owner = "hive:acct"
				seedLedger(state, r)
			}
			require.Equal(t, tc.expect, state.GetBalance("hive:acct", bh, tc.asset))
		})
	}
}
