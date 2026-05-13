package devnet

import (
	"context"
	"os"
	"testing"
	"time"

	"vsc-node/modules/common/params"
	systemconfig "vsc-node/modules/common/system-config"
)

// TestDashWithdrawalFlow is a scaffold for the end-to-end Dash withdrawal
// (unmap → buildSpendTransaction → broadcast) path. It is intentionally
// gated behind DASH_TEST_WITHDRAWAL=1 because the contract's spending path
// is still incomplete:
//
//   - HandleUnmap calls createP2SHAddressWithBackup for change addresses ✓
//   - buildSpendTransaction calls createP2SHAddressWithBackup for the
//     input UTXO redeem scripts ✓
//   - …but the signed transaction is still serialized as a SegWit witness
//     spend (TxIn.Witness, BIP143 sighash). Dash Core rejects SegWit
//     decode entirely, so the broadcast will fail with -25
//     "TX decode failed" or -26 "scriptpubkey".
//
// Until that signing path is reworked to produce a P2SH scriptSig (redeem
// script + signatures in scriptSig, BIP16 sighash, no witness section),
// running this test only documents the gap. Once the rework lands, drop
// the env-var gate and this test becomes the canonical proof that
// withdrawals work end-to-end on Dash regtest.
//
// Run with:
//
//	DASH_TEST_WITHDRAWAL=1 go test -v -run TestDashWithdrawalFlow -timeout 20m ./tests/devnet/
func TestDashWithdrawalFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping devnet Dash withdrawal flow in short mode")
	}
	if os.Getenv("DASH_TEST_WITHDRAWAL") != "1" {
		t.Skip("skipping until P2SH-signing rework lands; rerun with DASH_TEST_WITHDRAWAL=1 once buildSpendTransaction emits a P2SH scriptSig spend")
	}
	requireDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Minute)
	defer cancel()

	// ── Devnet bootstrap (same shape as TestDashDepositCreditFlow) ──────
	wasmPath, err := DashMappingContractPath()
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.Logf("dash-mapping-contract WASM: %s", wasmPath)

	cfg := DefaultConfig()
	cfg.Nodes = 5
	cfg.GenesisNode = 5
	cfg.LogLevel = "info"
	cfg.EnableDashd = true
	cfg.SysConfigOverrides = &systemconfig.SysConfigOverrides{
		ConsensusParams: &params.ConsensusParams{
			ElectionInterval: 60,
		},
	}
	if os.Getenv("DEVNET_KEEP") != "" {
		cfg.KeepRunning = true
	}

	d, err := New(cfg)
	if err != nil {
		t.Fatalf("creating devnet: %v", err)
	}
	t.Cleanup(func() { d.Stop() })

	if err := d.Start(ctx); err != nil {
		dumpLogs(t, d, ctx)
		t.Fatalf("starting devnet: %v", err)
	}

	if err := d.FundVSCBalance("magi.test1", "100.000"); err != nil {
		t.Fatalf("FundVSCBalance(magi.test1): %v", err)
	}
	time.Sleep(5 * time.Second)

	// ── INTENDED FLOW (to be wired up when signing rework lands) ────────
	//
	// 1. Deploy + initialise the contract (seedBlocks + registerPublicKey).
	// 2. Deposit DASH to a P2SH deposit address derived from the test keys
	//    (same as TestDashDepositCreditFlow/Deposit_Credits_HiveUser).
	// 3. Wait for the deposit to land as a contract balance (a-hive:<user>).
	// 4. Call `unmap` with {amount, to:<external Dash address>}. The
	//    contract should:
	//       - Burn the balance
	//       - Build a Dash-spending tx that consumes the deposit UTXO
	//       - Return the rawtx hex (P2SH-scriptSig, NO witness)
	// 5. Broadcast the rawtx via dash-cli sendrawtransaction. Dash Core
	//    should accept it.
	// 6. Mine a block and assert the recipient address received the funds
	//    (via dash-cli getreceivedbyaddress).
	// 7. Submit confirmSpend so the contract's UTXO accounting clears.
	//
	// The "should accept" step in (5) is currently the failure mode — the
	// rawtx today carries a BIP141 witness section that Dash rejects.

	t.Fatal("unmap flow not yet implemented; see TestDashWithdrawalFlow comment for the gating reason")
}
