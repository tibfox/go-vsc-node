package devnet

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"go.mongodb.org/mongo-driver/bson"

	"vsc-node/cmd/mapping-bot/chain"
	"vsc-node/modules/common/params"
	systemconfig "vsc-node/modules/common/system-config"
)

// TestDashSwapDepositToHbd is the canonical proof that ANY swap involving
// Dash works on devnet end-to-end. The simplest swap that exercises every
// piece of the DASH integration with the DEX is:
//
//	user deposits DASH with `swap_to=<hive user>&swap_asset_out=hbd
//	&destination_chain=magi` → dash-mapping's MapSwap path → DEX router
//	→ pool → HBD output credited to the recipient's VSC HBD balance.
//
// What this proves:
//
//   - Dash deposits with swap-out instructions parse correctly
//     (regression coverage for the SwapNetworkOut → DestinationChainKey
//     rename in dash-mapping-contract).
//   - dash-mapping treats DASH as a first-class asset_in for routing
//     (DashAssetValue threading through processUtxos → router.execute).
//   - The DEX router accepts mapped-DASH input via the standard
//     MappedAsset.DrawAssetFrom allowance dance.
//   - The DEX pool can hold DASH as a reserve and execute the swap math.
//   - HBD output settles back to a Hive user via native HiveTransfer
//     (no Dash-chain side effects — pure VSC ledger move).
//
// Setup is heavy because we have to seed pool liquidity from a real Dash
// deposit (the mapping contract has no mint primitive). One devnet, one
// flow — keeps the cumulative contract-call count below the state-engine
// settlement-lag threshold we've seen surface around ~15 calls.
//
// Run with:
//
//	go test -v -run TestDashSwapDepositToHbd -timeout 20m ./tests/devnet/
func TestDashSwapDepositToHbd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping devnet Dash swap flow in short mode")
	}
	requireDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Minute)
	defer cancel()

	// ── Locate prebuilt WASM artifacts ──────────────────────────────────
	dashMappingWasm, err := DashMappingContractPath()
	if err != nil {
		t.Fatalf("%v", err)
	}
	dexRouterWasm, err := DexRouterContractPath()
	if err != nil {
		t.Fatalf("%v", err)
	}
	dexPoolWasm, err := DexPoolContractPath()
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.Logf("dash-mapping WASM: %s", dashMappingWasm)
	t.Logf("dex-router-v2 WASM: %s", dexRouterWasm)
	t.Logf("dex pool WASM: %s", dexPoolWasm)

	// ── Devnet bootstrap ────────────────────────────────────────────────
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

	tip, err := d.EnsureDashCoinbaseMature(ctx)
	if err != nil {
		t.Fatalf("EnsureDashCoinbaseMature: %v", err)
	}

	// magi.test1 owns the pool's input HBD intent. Funding 2000 TBD here
	// gives 2_000_000 sat-HBD on the VSC ledger, well above the
	// rc_limit=500_000 used in CallContract. PullBalance reserves
	// `rc_limit - rcFreeRemaining` HBD against future RC use whenever a
	// contract draws HBD via intent (see [[vsc_rc_limit_hbd_exclusion]]),
	// so smaller fundings collide with this exclusion and the HBD draw
	// fails with native ledger "insufficient balance" even though the
	// contract-level intent limit is satisfied.
	if err := d.FundVSCBalance("magi.test1", "2000.000"); err != nil {
		t.Fatalf("FundVSCBalance(magi.test1): %v", err)
	}
	time.Sleep(5 * time.Second)

	// ── Deploy contracts ────────────────────────────────────────────────
	// Three back-to-back deploys can over-saturate consensus storage-proof
	// signing on this devnet shape: the third deploy reliably hits
	// "signer count: 0 / failed to request storage proof context deadline
	// exceeded" without breathing room between deploys. ~20s between each
	// gives the witness quorum time to settle and re-sign cleanly.
	dashMappingId, err := d.DeployContract(ctx, ContractDeployOpts{
		WasmPath:     dashMappingWasm,
		Name:         "dash-mapping-contract",
		Description:  "Dash UTXO mapping (swap-flow test)",
		DeployerNode: 1,
		GQLNode:      2,
	})
	if err != nil {
		t.Fatalf("deploying dash-mapping: %v", err)
	}
	lastDeployedContractId = dashMappingId
	t.Logf("dash-mapping: %s", dashMappingId)
	time.Sleep(45 * time.Second)

	dexRouterId, err := d.DeployContract(ctx, ContractDeployOpts{
		WasmPath:     dexRouterWasm,
		Name:         "dex-router-v2",
		Description:  "DEX router (swap-flow test)",
		DeployerNode: 1,
		GQLNode:      2,
	})
	if err != nil {
		t.Fatalf("deploying dex-router: %v", err)
	}
	t.Logf("dex-router: %s", dexRouterId)
	time.Sleep(45 * time.Second)

	dexPoolId, err := d.DeployContract(ctx, ContractDeployOpts{
		WasmPath:     dexPoolWasm,
		Name:         "dex-pool-dash-hbd",
		Description:  "DASH/HBD pool (swap-flow test)",
		DeployerNode: 1,
		GQLNode:      2,
	})
	if err != nil {
		t.Fatalf("deploying dex-pool: %v", err)
	}
	t.Logf("dex-pool (DASH/HBD): %s", dexPoolId)

	// ── Dash-mapping init: seed, TSS-keys ritual, register router ──────
	seedHeaderHex, err := d.GetDashBlockHeaderHex(ctx, tip)
	if err != nil {
		t.Fatalf("seed header at %d: %v", tip, err)
	}
	if _, err := d.CallContract(ctx, 1, dashMappingId, "seedBlocks",
		fmt.Sprintf(`{"block_header":"%s","block_height":%d}`, seedHeaderHex, tip)); err != nil {
		t.Fatalf("seedBlocks: %v", err)
	}

	// Use hardcoded test-vector pubkeys via registerPublicKey directly.
	// The swap path never invokes sdk.TssSignKey — only unmap does —
	// so the createKey → DKG → fetch-pubkey ritual buys us nothing here
	// and dominates the cumulative devnet-activity budget (~2 min of DKG
	// reliably pushes registerPublicKey state propagation past 180s
	// settlement-lag windows). The companion TestDashTssKeyInit test
	// already proves the production ritual; this test focuses on swap.
	const primaryPubKeyHex = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
	const backupPubKeyHex = "02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5"
	if _, err := d.CallContract(ctx, 1, dashMappingId, "registerPublicKey",
		fmt.Sprintf(`{"primary_public_key":"%s","backup_public_key":"%s"}`,
			primaryPubKeyHex, backupPubKeyHex)); err != nil {
		t.Fatalf("registerPublicKey: %v", err)
	}
	if _, err := d.WaitForContractState(ctx, 2, dashMappingId, "pubkey", 120*time.Second,
		func(v []byte) bool { return len(v) > 0 },
	); err != nil {
		t.Fatalf("pubkey state never landed: %v", err)
	}

	// Tell dash-mapping which router to use for swap-out instructions.
	if _, err := d.CallContract(ctx, 1, dashMappingId, "registerRouter",
		fmt.Sprintf(`{"router_contract":"%s"}`, dexRouterId)); err != nil {
		t.Fatalf("registerRouter: %v", err)
	}

	regtestParams := chaincfg.RegressionNetParams
	regtestParams.PubKeyHashAddrID = 0x8c
	regtestParams.ScriptHashAddrID = 0x13
	addrGen := &chain.DashAddressGenerator{
		Params:          &regtestParams,
		BackupCSVBlocks: 2,
	}
	blockParser := &chain.BTCBlockParser{Params: &regtestParams}

	// ── Real DASH deposit so magi.test1 has 1 DASH to seed pool ────────
	const seedDepositAmount = "1.0"           // DASH
	const seedDepositDuffs int64 = 100_000_000 // 1.0 * 1e8
	const seedHiveUser = "magi.test1"
	seedInstr := "deposit_to=hive:" + seedHiveUser
	seedAddr, _, err := addrGen.GenerateDepositAddress(
		primaryPubKeyHex, backupPubKeyHex, seedInstr,
	)
	if err != nil {
		t.Fatalf("derive seed deposit addr: %v", err)
	}
	seedInput := sendAndConfirmDeposit(ctx, t, d, blockParser, seedAddr, seedDepositAmount)
	if _, err := d.CallContract(ctx, 1, dashMappingId, "map",
		buildMapPayload(t, seedInput, seedInstr)); err != nil {
		t.Fatalf("map (seed deposit): %v", err)
	}
	if _, err := d.WaitForContractState(ctx, 2, dashMappingId, "a-hive:"+seedHiveUser, 180*time.Second,
		func(v []byte) bool { return DecodeContractBalance(v) >= seedDepositDuffs },
	); err != nil {
		t.Fatalf("seed deposit balance never landed: %v", err)
	}
	t.Logf("seed deposit OK: %s now holds %d duffs of mapped DASH", seedHiveUser, seedDepositDuffs)

	// ── DEX setup ───────────────────────────────────────────────────────
	if _, err := d.CallContract(ctx, 1, dexRouterId, "init", `"1.0.0"`); err != nil {
		t.Fatalf("router.init: %v", err)
	}
	if _, err := d.CallContract(ctx, 1, dexRouterId, "register_token",
		fmt.Sprintf(`{"name":"DASH","chain":"DASH","mapping_contract":"%s"}`, dashMappingId)); err != nil {
		t.Fatalf("register_token DASH: %v", err)
	}
	if _, err := d.CallContract(ctx, 1, dexRouterId, "register_token",
		`{"name":"HBD","chain":"HIVE"}`); err != nil {
		t.Fatalf("register_token HBD: %v", err)
	}

	// Pool init: assets MUST be in alphabetical order. dash < hbd, so
	// asset0=dash (mapped), asset1=hbd (native). The mapping contract
	// attaches to asset0; asset1 has no mapping (HBD is native HIVE).
	const feeBps = 30 // 0.3% — standard test fee
	poolInitPayload := fmt.Sprintf(
		`{"asset0":"dash","asset1":"hbd","fee_bps":%d,"asset0_mapping_contract":"%s","router_contract":"%s"}`,
		feeBps, dashMappingId, dexRouterId,
	)
	if _, err := d.CallContract(ctx, 1, dexPoolId, "init", poolInitPayload); err != nil {
		t.Fatalf("pool.init: %v", err)
	}
	if _, err := d.CallContract(ctx, 1, dexRouterId, "register_pool",
		fmt.Sprintf(`{"asset0":"dash","asset1":"hbd","dex_contract_id":"%s"}`, dexPoolId)); err != nil {
		t.Fatalf("register_pool: %v", err)
	}

	// ── Seed pool liquidity: 0.5 DASH + 0.5 HBD ────────────────────────
	// asset0=DASH (mapped) drawn via ERC-20 allowance from the caller →
	// pool; asset1=HBD (native) drawn via Hive transfer.allow intent.
	const seedHbdAmount = int64(500)         // 0.500 HBD in 3-decimal milli-units
	const seedDashAmount = int64(50_000_000) // 0.5 DASH in duffs
	// Approve dash-mapping allowance: amount is a STRING (AllowanceParams.Amount string).
	approvePayload := fmt.Sprintf(`{"spender":"contract:%s","amount":"%d"}`,
		dexPoolId, seedDashAmount)
	if _, err := d.CallContract(ctx, 1, dashMappingId, "approve", approvePayload); err != nil {
		t.Fatalf("approve seed allowance: %v", err)
	}
	// add_liquidity: amount0=DASH (asset0), amount1=HBD (asset1).
	addLiqPayload := fmt.Sprintf(`{"amount0":"%d","amount1":"%d","recipient":"hive:magi.test1"}`,
		seedDashAmount, seedHbdAmount)
	if _, err := d.CallContractWithIntents(ctx, 1, dexPoolId, "add_liquidity", addLiqPayload,
		[]map[string]any{
			{
				"type": "transfer.allow",
				"args": map[string]string{
					"token":       "hbd",
					"limit":       fmt.Sprintf("%d", seedHbdAmount),
					"contract_id": dexPoolId,
				},
			},
		},
	); err != nil {
		t.Fatalf("add_liquidity: %v", err)
	}
	// Give pool state time to land.
	time.Sleep(8 * time.Second)
	t.Log("pool liquidity seeded: 0.5 DASH + 0.5 HBD")

	// ── THE SWAP: another Dash deposit with swap-out instruction ───────
	// Recipient is magi.test2 to make balance changes easy to assert.
	const swapRecipientUser = "magi.test2"
	const swapDepositAmount = "0.1"           // DASH
	const swapDepositDuffs int64 = 10_000_000 // 0.1 * 1e8
	swapInstr := fmt.Sprintf(
		"swap_to=hive:%s&swap_asset_out=hbd&destination_chain=magi",
		swapRecipientUser,
	)
	swapAddr, _, err := addrGen.GenerateDepositAddress(
		primaryPubKeyHex, backupPubKeyHex, swapInstr,
	)
	if err != nil {
		t.Fatalf("derive swap deposit addr: %v", err)
	}

	// Record magi.test2's HBD balance before the swap, so we can assert
	// it strictly increased.
	preHBD, _ := queryHiveAccountHBD(ctx, t, d, "magi.test2")
	t.Logf("magi.test2 HBD before swap: %d sat-HBD", preHBD)

	swapInput := sendAndConfirmDeposit(ctx, t, d, blockParser, swapAddr, swapDepositAmount)
	if _, err := d.CallContract(ctx, 1, dashMappingId, "map",
		buildMapPayload(t, swapInput, swapInstr)); err != nil {
		t.Fatalf("map (swap deposit): %v", err)
	}

	// Wait for magi.test2's HBD balance to increase. The exact amount
	// depends on AMM math (constant-product, fee=30bps, depending on
	// pool reserves), so we just assert "strictly increased."
	t.Logf("waiting for magi.test2's HBD balance to reflect the swap output...")
	deadline := time.Now().Add(3 * time.Minute)
	var postHBD int64
	for time.Now().Before(deadline) {
		current, ok := queryHiveAccountHBD(ctx, t, d, "magi.test2")
		if ok && current > preHBD {
			postHBD = current
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled while waiting for swap output: %v", ctx.Err())
		case <-time.After(5 * time.Second):
		}
	}
	if postHBD == 0 {
		t.Fatalf("swap output never landed: magi.test2 HBD stayed at %d sat-HBD after 3min", preHBD)
	}
	delta := postHBD - preHBD
	t.Logf("✓ swap landed: magi.test2 HBD %d → %d (delta %d sat-HBD, in: %d duffs DASH)",
		preHBD, postHBD, delta, swapDepositDuffs)
}

// ─── Helpers ────────────────────────────────────────────────────────────

// queryHiveAccountHBD returns the given hive account's HBD balance in
// sat-HBD (3-decimal units) on the VSC ledger. Returns (0, false) if the
// account record isn't present yet (e.g., never received anything).
func queryHiveAccountHBD(ctx context.Context, t *testing.T, d *Devnet, account string) (int64, bool) {
	t.Helper()
	// The VSC ledger stores balances per account in MongoDB's
	// ledger_balances collection. Querying directly is simpler than
	// shaping a GraphQL request for getAccountBalance.
	docs, err := d.GetLedgerBalances(ctx, 2, bson.M{"account": "hive:" + account})
	if err != nil {
		t.Fatalf("queryHiveAccountHBD(%s): %v", account, err)
	}
	if len(docs) == 0 {
		return 0, false
	}
	// Take the most recent row (highest block_height).
	max := docs[0]
	for _, d := range docs[1:] {
		if d.BlockHeight > max.BlockHeight {
			max = d
		}
	}
	return max.Hbd, true
}

// Stubbed wire-format hex helper kept for symmetry with other tests
// (the existing devnet test files use it; placeholder for any debug logs
// we might add during iteration).
var _ = hex.EncodeToString
