package ledgerSystem_test

// review7 C7-b (yecier) — a withdrawal whose destination is the gateway's own
// Hive account (vsc.gateway) is accepted, debiting the user on L2 while the L1
// funds are routed straight back into the gateway multisig with no credit to
// anyone — the funds are stranded. The destination gate must reject it.

import (
	"testing"

	ledgerDb "vsc-node/modules/db/vsc/ledger"
	ledgerSystem "vsc-node/modules/ledger-system"

	"github.com/stretchr/testify/require"
)

func TestAuditReview7_C7b_WithdrawToGatewayRejected(t *testing.T) {
	const bh = uint64(100)

	newFunded := func() *ledgerSystem.LedgerState {
		s := newTestState()
		seedLedger(s, ledgerDb.LedgerRecord{
			Id: "dep", Owner: "hive:alice", Amount: 100000, Asset: "hbd",
			Type: "deposit", BlockHeight: 10,
		})
		return s
	}

	withdrawTo := func(to string) ledgerSystem.LedgerResult {
		session := ledgerSystem.NewSession(newFunded())
		defer session.Revert()
		return session.Withdraw(ledgerSystem.WithdrawParams{
			Id: "w", From: "hive:alice", To: to, Amount: 1000, Asset: "hbd", BlockHeight: bh,
		})
	}

	// Control: a withdrawal to an ordinary Hive account succeeds.
	require.True(t, withdrawTo("someacct").Ok, "control withdrawal to a normal account should succeed")

	// C7-b: the gateway's own account must be rejected, in both spellings.
	require.False(t, withdrawTo("vsc.gateway").Ok,
		"withdraw to vsc.gateway must be rejected — it strands the user's funds")
	require.False(t, withdrawTo("hive:vsc.gateway").Ok,
		"withdraw to hive:vsc.gateway must be rejected")
}
