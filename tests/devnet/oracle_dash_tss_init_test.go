package devnet

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"vsc-node/modules/common/params"
	systemconfig "vsc-node/modules/common/system-config"
)

// TestDashTssKeyInit exercises the production deploy ritual for the
// dash-mapping-contract in isolation:
//
//	deploy → seedBlocks → createKey → WaitForTssKey(active)
//	      → registerPublicKey(primary=<TSS pubkey>, backup=<cold key>)
//	      → assert contract state has `pubkey` populated
//
// This is the path BTC mainnet deployers already follow (and any chain
// whose deployer skips it will see working deposits but silently-stalled
// withdrawals — TSS has no share to sign with). The test exists as a
// standalone proof that the ritual works end-to-end on our devnet,
// without the deposit-credit / map / pause subtests that pile up enough
// devnet activity to surface unrelated consensus-drift flake on node 2.
//
// Lives in its own test so each run gets a fresh devnet — keeps the
// per-devnet contract-call count low (just seedBlocks + createKey +
// registerPublicKey, three calls total) and well below the threshold
// where state-engine settlement on node 2 starts lagging.
//
// Run with:
//
//	go test -v -run TestDashTssKeyInit -timeout 12m ./tests/devnet/
func TestDashTssKeyInit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping devnet Dash TSS-init flow in short mode")
	}
	requireDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

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

	// Mature dashd's coinbase so seedBlocks has a header to plant at.
	tip, err := d.EnsureDashCoinbaseMature(ctx)
	if err != nil {
		t.Fatalf("EnsureDashCoinbaseMature: %v", err)
	}
	t.Logf("dashd tip after coinbase maturity: %d", tip)

	// Top up deployer RC. Only three contract calls here so 100 TBD is
	// plenty (see TestDashDepositCreditFlow for the longer-form sizing
	// rationale), but stay consistent with the other test for clarity.
	if err := d.FundVSCBalance("magi.test1", "100.000"); err != nil {
		t.Fatalf("FundVSCBalance(magi.test1): %v", err)
	}
	time.Sleep(5 * time.Second)

	contractId, err := d.DeployContract(ctx, ContractDeployOpts{
		WasmPath:     wasmPath,
		Name:         "dash-mapping-contract",
		Description:  "Dash UTXO mapping (devnet TSS-init test)",
		DeployerNode: 1,
		GQLNode:      2,
	})
	if err != nil {
		t.Fatalf("deploying contract: %v", err)
	}
	t.Logf("contract deployed: %s", contractId)

	// seedBlocks first — checkOracle on the testnet build accepts the
	// contract owner, so magi.test1 can call admin-gated actions.
	seedHeaderHex, err := d.GetDashBlockHeaderHex(ctx, tip)
	if err != nil {
		t.Fatalf("seed header at %d: %v", tip, err)
	}
	seedPayload := fmt.Sprintf(`{"block_header":"%s","block_height":%d}`, seedHeaderHex, tip)
	if _, err := d.CallContract(ctx, 1, contractId, "seedBlocks", seedPayload); err != nil {
		t.Fatalf("seedBlocks: %v", err)
	}
	t.Logf("seedBlocks(height=%d) submitted", tip)

	// ── Step 1: createKey kicks off TSS DKG under constants.TssKeyName.
	if _, err := d.CallContract(ctx, 1, contractId, "createKey", `""`); err != nil {
		t.Fatalf("createKey: %v", err)
	}
	t.Log("createKey submitted; waiting for TSS DKG to complete...")

	// ── Step 2: poll until the key record reaches status=active and
	// has a public_key. WaitForTssKey returns as soon as a document
	// matching the filter exists; the status=active part of the filter
	// ensures we don't read it before DKG finishes.
	fullKeyId := contractId + "-main"
	keyDoc, err := d.WaitForTssKey(ctx, 2,
		bson.M{"id": fullKeyId, "status": "active"}, 5*time.Minute)
	if err != nil {
		t.Fatalf("WaitForTssKey(%s, active): %v", fullKeyId, err)
	}
	if keyDoc.PublicKey == "" {
		t.Fatalf("TSS key %s reached status=active but public_key is empty", fullKeyId)
	}
	primaryPubKeyHex := keyDoc.PublicKey
	t.Logf("TSS DKG complete: primary pubkey = %s (len=%d)", primaryPubKeyHex, len(primaryPubKeyHex))
	if want := 66; len(primaryPubKeyHex) != want {
		t.Errorf("primary pubkey hex length: got %d, want %d (compressed secp256k1)",
			len(primaryPubKeyHex), want)
	}

	// ── Step 3: registerPublicKey with the TSS-derived primary + a
	// static cold-key backup. The contract creates only ONE TSS key
	// (TssKeyName = "main"); backup is the CSV-gated emergency branch
	// and uses an external pubkey by design.
	const backupPubKeyHex = "02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5"
	regKeysPayload := fmt.Sprintf(`{"primary_public_key":"%s","backup_public_key":"%s"}`,
		primaryPubKeyHex, backupPubKeyHex)
	if _, err := d.CallContract(ctx, 1, contractId, "registerPublicKey", regKeysPayload); err != nil {
		t.Fatalf("registerPublicKey: %v", err)
	}
	t.Log("registerPublicKey submitted; waiting for state to land...")

	// ── Step 4: assert the `pubkey` state key materialized — this is
	// what the address-derivation paths (parseInstructions in the
	// contract, GenerateDepositAddress on the bot) read. If it never
	// lands, the ritual isn't complete.
	pubkeyState, err := d.WaitForContractState(ctx, 2, contractId, "pubkey", 60*time.Second,
		func(v []byte) bool { return len(v) > 0 },
	); if err != nil {
		t.Fatalf("`pubkey` state never landed: %v", err)
	}
	if len(pubkeyState) == 0 {
		t.Fatalf("`pubkey` state read empty after register")
	}
	t.Logf("`pubkey` state landed (%d bytes); ritual complete", len(pubkeyState))

	// Also sanity-check the `backupkey` half lands, since registerPublicKey
	// sets both in the same WASM call.
	backupkeyState, err := d.WaitForContractState(ctx, 2, contractId, "backupkey", 60*time.Second,
		func(v []byte) bool { return len(v) > 0 },
	)
	if err != nil {
		t.Fatalf("`backupkey` state never landed: %v", err)
	}
	if len(backupkeyState) == 0 {
		t.Fatalf("`backupkey` state read empty after register")
	}
	t.Logf("`backupkey` state landed (%d bytes)", len(backupkeyState))
}
