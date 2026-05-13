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
// gated behind DASH_TEST_WITHDRAWAL=1.
//
// Status of the prerequisites:
//
//   - HandleUnmap derives change addresses as P2SH ✓
//   - buildSpendTransaction derives input redeem scripts via
//     createP2SHAddressWithBackup ✓
//   - signSpendTransaction produces BIP16 (legacy) sighashes via
//     txscript.CalcSignatureHash, NOT BIP143 witness sighashes ✓
//   - calculateP2SHFee accounts for non-SegWit scriptSig overhead ✓
//   - The bot's attachSignatures assembles a P2SH scriptSig
//     (push(sig+hashtype) push(branch) push(redeem)) and serializes
//     in wire.BaseEncoding (no SegWit marker+flag bytes) on Dash ✓
//
// What still blocks a fully unattended end-to-end run:
//
//   - This test currently registers hardcoded test-vector pubkeys via
//     registerPublicKey, NOT TSS-generated keys. The contract emits a
//     real `sdk.TssSignKey` request when unmap runs, but the TSS
//     service has no matching private share for the test-vector keys,
//     so signatures never come back via getTssRequests.
//   - To exercise a real broadcast, the test must instead call the
//     `createKey` action (which goes through sdk.TssCreateKey), wait
//     for the TSS round to complete (~20–60 s in devnet), fetch the
//     resulting pubkeys from the TSS service via GraphQL, then call
//     registerPublicKey with those TSS-owned pubkeys so the deposit
//     address derivation lines up with shares the network can sign
//     against.
//
// Run with:
//
//	DASH_TEST_WITHDRAWAL=1 go test -v -run TestDashWithdrawalFlow -timeout 20m ./tests/devnet/
func TestDashWithdrawalFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping devnet Dash withdrawal flow in short mode")
	}
	if os.Getenv("DASH_TEST_WITHDRAWAL") != "1" {
		t.Skip("skipping until the test is wired up to use TSS-generated keys via createKey + registerPublicKey-with-TSS-pubkey; rerun with DASH_TEST_WITHDRAWAL=1 once that setup is implemented")
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

	// ── INTENDED FLOW (to be wired up when TSS-key setup is added) ─────
	//
	// 1. Deploy the contract; call `createKey` (which invokes
	//    sdk.TssCreateKey under constants.TssKeyName), wait for the TSS
	//    round to land, fetch the generated pubkeys via the TSS GraphQL
	//    API, and finally call `registerPublicKey` with those TSS-owned
	//    pubkeys. Both primary and backup are needed for the OP_IF/OP_ELSE
	//    branches in the redeem script.
	// 2. Seed the contract at the dashd tip.
	// 3. Top up magi.test1's VSC HBD ledger via FundVSCBalance (≥ 500 TBD)
	//    so the multi-call withdrawal flow doesn't blow the RC budget.
	// 4. Deposit DASH to the P2SH deposit address derived from the TSS
	//    pubkeys (same shape as TestDashDepositCreditFlow/Deposit_Credits_HiveUser).
	// 5. Wait for the deposit to land as a contract balance (a-hive:<user>).
	// 6. Call `unmap` with {amount, to:<external Dash regtest address>}.
	//    Contract:
	//       - Burns the balance
	//       - Builds a P2SH-spending tx that consumes the deposit UTXO
	//       - Emits TssSignKey requests for each input's sighash
	//       - Stores SigningData under d-<txid> in contract state
	// 7. Poll getTssRequests until every sighash has status=complete and
	//    a signature is available. The TSS service in devnet will sign
	//    automatically once the key shares produced in step 1 are usable.
	// 8. Run the bot's attachSignatures equivalent: assemble the scriptSig
	//    push(sig+hashtype, branch, redeem_script) and serialize with
	//    wire.BaseEncoding (no witness).
	// 9. Broadcast via dash-cli sendrawtransaction. Dash Core should
	//    accept it (P2SH scriptSig spending a P2SH-wrapped redeem script).
	// 10. Mine a block. Verify the recipient address received the funds via
	//    dash-cli getreceivedbyaddress.
	// 11. Submit confirmSpend so the contract's UTXO accounting clears.

	t.Fatal("unmap flow not yet implemented; see TestDashWithdrawalFlow comment for the TSS-key wiring gap")
}
