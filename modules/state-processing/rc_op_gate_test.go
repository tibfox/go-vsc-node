package state_engine_test

import (
	"encoding/json"
	"testing"

	stateEngine "vsc-node/modules/state-processing"

	"github.com/stretchr/testify/require"
)

// RC-UNGATED-OPS-1: a contract call is refused without the 100 RC minimum. A
// transfer, withdraw, stake or unstake was not refused for anything: each one
// charges a fixed RC when it is done, and nothing asked first whether the payer
// had it. The charge landed on a frozen total the end-of-slot clamp had already
// pinned at the payer's ceiling, where it changed nothing — so an account with
// no RC left kept transferring, for ever, for free. RC is the only thing
// rationing these operations.
//
// These drive real L1 blocks through the engine, so they exercise the whole
// path: the gate in ExecuteBatch, the charge that follows it, and the RC record
// the slot writes.

// rcOpGateEnv is alice with a small HBD balance and no RC left to spend: her
// frozen total is seeded at her ceiling (balance + the free allowance), which
// returns at 0.07 RC a block — she cannot reach the 100 RC a transfer costs
// within the handful of blocks these tests run.
func rcOpGateEnv(t *testing.T, consensus uint64) *testEnv {
	t.Helper()
	te := newTestEnv()
	te.ElectionDb.ElectionsByHeight[1] = versionedElection(1, 1, 0, consensus)
	te.Creator.Transfer("alice", "vsc.gateway", "10", "HBD", "")
	te.processAndWait()

	h := te.Reader.LastBlock
	bal := te.SE.LedgerSystem.GetBalance("hive:alice", h, "hbd")
	require.Greater(t, bal, int64(0), "alice must hold something to transfer; the point is RC, not balance")
	ceiling := bal + te.SE.RcSystem.HiveFreeAmount
	require.NoError(t, te.RcDb.SetRecord("hive:alice", h, ceiling))
	require.Equal(t, int64(0), te.SE.RcSystem.GetAvailableRCs("hive:alice", h),
		"the test needs alice out of RC with her balance intact")
	return te
}

// rcOpGateTransfer sends 5 of alice's 10 base units to bob and settles the
// slot. The amount is affordable: what is being tested is the RC, not the
// balance.
func rcOpGateTransfer(te *testEnv) string {
	j, _ := json.Marshal(map[string]interface{}{
		"from":   "hive:alice",
		"to":     "hive:bob",
		"amount": "0.005",
		"asset":  "hbd",
		"memo":   "",
		"net_id": "vsc-mocknet",
	})
	id := te.Creator.CustomJson(stateEngine.MockJson{
		RequiredAuths: []string{"alice"},
		Id:            "vsc.transfer",
		Json:          string(j),
	}).Id
	te.processAndWait()
	// Two slots: the engine executes a slot's batch when it rolls into the next
	// one, and folds that batch's RC map into the RC records when it rolls into
	// the one after. One settle leaves the charge in se.RcMap, unwritten.
	te.settleSlot()
	te.settleSlot()
	return id
}

func TestRcOpGate_ATransferByAPayerWithNoRcIsRefused(t *testing.T) {
	te := rcOpGateEnv(t, 18) // 0.18.0 active
	before := rcRecords(te, "hive:alice")

	id := rcOpGateTransfer(te)

	require.False(t, te.SE.TxOutput[id].Ok,
		"a transfer by a payer with no RC must not execute: RC is the only thing rationing it")
	require.Contains(t, te.TxDb.Records[id].Error, "not enough RCs available",
		"the refusal must say what it was, so the payer can see why")
	require.Contains(t, te.TxDb.Records[id].Error, "transfer costs 100 RC",
		"and name the price it could not meet")

	// Charged nothing for the refusal, so her RC record is untouched and the
	// return she has already earned keeps running (RC-TREADMILL-1).
	require.Equal(t, before, rcRecords(te, "hive:alice"),
		"a refusal must not be charged: charging for one is what pins an account at zero")
	t.Logf("refused: %q", te.TxDb.Records[id].Error)
}

// The control, and the reason the case above is not vacuous: a payer who can
// pay is still charged in full and the operation still runs. A fix that simply
// stopped transfers, or stopped charging for them, passes the case above and
// fails this one.
func TestRcOpGate_APayerWhoCanPayIsStillChargedInFull(t *testing.T) {
	te := newTestEnv()
	te.ElectionDb.ElectionsByHeight[1] = versionedElection(1, 1, 0, 18)
	te.Creator.Transfer("alice", "vsc.gateway", "10", "HBD", "")
	te.processAndWait()
	require.Empty(t, rcRecords(te, "hive:alice"), "alice has not been charged for anything yet")

	id := rcOpGateTransfer(te)

	require.True(t, te.SE.TxOutput[id].Ok, "alice has her whole free allowance; her transfer must go through")
	require.Empty(t, te.TxDb.Records[id].Error)
	recs := rcRecords(te, "hive:alice")
	require.Len(t, recs, 1, "the slot must have written her RC record")
	require.Equal(t, int64(100), recs[0].Amount, "a transfer costs 100 RC and she paid it")
	t.Logf("charged: %+v", recs)
}

// Below the line the operation runs unpriced, exactly as it always did, so a
// node re-executing settled history reaches the same balances — and this is the
// bug in the form it shipped: the transfer succeeds with no RC at all, and the
// 100 RC it is charged lands on a total already pinned at her ceiling, where it
// buys no deterrence and only re-anchors her return.
func TestRcOpGate_BelowTheLineATransferWithNoRcStillRuns(t *testing.T) {
	te := rcOpGateEnv(t, 17) // 0.17.0: before the line
	seeded := rcRecords(te, "hive:alice")
	require.Len(t, seeded, 1)

	id := rcOpGateTransfer(te)

	require.True(t, te.SE.TxOutput[id].Ok,
		"below 0.18.0 a transfer by a payer with no RC must still execute; settled history depends on it")
	recs := rcRecords(te, "hive:alice")
	require.Greater(t, len(recs), 1, "below the line the charge still writes her record")
	last := recs[len(recs)-1]
	require.Greater(t, last.BlockHeight, seeded[0].BlockHeight,
		"and re-anchors it at the closing slot, which is what restarts her return from zero")
	t.Logf("below the line: ran with no RC, records %+v", recs)
}
