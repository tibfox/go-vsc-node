package devnet

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg"

	"vsc-node/cmd/mapping-bot/chain"
	"vsc-node/modules/common/params"
	systemconfig "vsc-node/modules/common/system-config"
)

// Well-known secp256k1 test vectors used as the contract's primary/backup
// pubkeys. Same vectors used by dash-mapping-contract/tests/current — so
// derived addresses match between the on-chain validation and the
// off-chain bot helpers.
const (
	testPrimaryPubKeyHex = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
	testBackupPubKeyHex  = "02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5"
)

// TestDashDepositCreditFlow exercises the full deposit-credit pipeline in
// a self-contained regtest devnet — proving that:
//
//   - The contract initializes correctly (registerPublicKey + seedBlocks)
//   - Bot-side address derivation (cmd/mapping-bot/chain.BTCAddressGenerator)
//     produces the same address the contract validates against
//   - A real Dash regtest tx paying that address can be confirmed in a
//     mined block
//   - The contract accepts the block header via addBlocks
//   - The bot-style SPV proof (cmd/mapping-bot/chain.BTCBlockParser) verifies
//     against that header and credits the Hive account named in the
//     deposit's instruction string
//
// Three subtests share one devnet to amortize startup cost (~30s):
//
//   - "Deposit_Credits_HiveUser":      happy path
//   - "Map_Idempotent_On_Retry":       second `map` for same tx is a no-op (no double credit)
//   - "MapInstantSend_Then_Map_NoDoubleCredit": IS-locked fast credit, then
//     normal map for the same tx after block confirms — credit happens once
//     and the marker upgrades from "1" → "2"
//
// Bypasses the oracle service: this test calls `addBlocks` directly from
// the contract-owner identity (which checkOracle accepts on testnet
// builds). The full oracle-driven relay path is covered by the existing
// TestOracleDashChainRelay.
//
// Run with:
//
//	go test -v -run TestDashDepositCreditFlow -timeout 20m ./tests/devnet/
func TestDashDepositCreditFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping devnet Dash deposit-credit flow in short mode")
	}
	requireDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Minute)
	defer cancel()

	// ── Locate prebuilt contract WASM ────────────────────────────────────
	wasmPath, err := DashMappingContractPath()
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.Logf("dash-mapping-contract WASM: %s", wasmPath)

	// ── Spin up devnet ───────────────────────────────────────────────────
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

	// ── Mature dashd's coinbase so the wallet has spendable funds ────────
	tip, err := d.EnsureDashCoinbaseMature(ctx)
	if err != nil {
		t.Fatalf("EnsureDashCoinbaseMature: %v", err)
	}
	t.Logf("dashd tip after coinbase maturity: %d", tip)

	// ── Deploy + initialize the contract ─────────────────────────────────
	contractId, err := d.DeployContract(ctx, ContractDeployOpts{
		WasmPath:     wasmPath,
		Name:         "dash-mapping-contract",
		Description:  "Dash UTXO mapping (devnet credit-flow test)",
		DeployerNode: 1,
		GQLNode:      2,
	})
	if err != nil {
		t.Fatalf("deploying contract: %v", err)
	}
	t.Logf("contract deployed: %s", contractId)
	lastDeployedContractId = contractId // make available to confirmAndExtract

	// Seed the contract at the current tip — that way later block-relay
	// payloads just need to chain ONE header on top, keeping the test fast.
	seedHeight := tip
	seedHeaderHex, err := d.GetDashBlockHeaderHex(ctx, seedHeight)
	if err != nil {
		t.Fatalf("seed header at %d: %v", seedHeight, err)
	}
	seedPayload := fmt.Sprintf(`{"block_header":"%s","block_height":%d}`, seedHeaderHex, seedHeight)
	if _, err := d.CallContract(ctx, 1, contractId, "seedBlocks", seedPayload); err != nil {
		t.Fatalf("seedBlocks: %v", err)
	}
	t.Logf("seedBlocks(height=%d) submitted", seedHeight)

	// registerPublicKey is owner-only — magi-1 is the deployer/owner.
	regKeysPayload := fmt.Sprintf(`{"primary_pub_key":"%s","backup_pub_key":"%s"}`,
		testPrimaryPubKeyHex, testBackupPubKeyHex)
	if _, err := d.CallContract(ctx, 1, contractId, "registerPublicKey", regKeysPayload); err != nil {
		t.Fatalf("registerPublicKey: %v", err)
	}
	t.Log("registerPublicKey submitted; waiting for state propagation...")

	// Wait for the contract state to reflect the registered keys before
	// proceeding (otherwise the first map() race-loses against seedBlocks
	// state and aborts with "missing public keys").
	if _, err := d.WaitForContractState(ctx, 2, contractId, "pubkey", 60*time.Second,
		func(v []byte) bool { return len(v) > 0 },
	); err != nil {
		t.Fatalf("pubkey state never landed: %v", err)
	}
	t.Log("registerPublicKey state confirmed on chain")

	// ── Pre-compute the regtest address generator ───────────────────────
	//
	// MUST use chaincfg.RegressionNetParams (not the testnet Dash overrides)
	// because the contract's regtest build branch uses vanilla regtest
	// params via dashRegtestParams. Address bytes must round-trip.
	addrGen := &chain.BTCAddressGenerator{
		Params:          &chaincfg.RegressionNetParams,
		BackupCSVBlocks: 2, // matches dash-mapping-contract regtest BackupCSVBlocks
	}
	blockParser := &chain.BTCBlockParser{Params: &chaincfg.RegressionNetParams}

	// ── Subtests ─────────────────────────────────────────────────────────

	t.Run("Deposit_Credits_HiveUser", func(t *testing.T) {
		const hiveUser = "magi-2"
		const instruction = "deposit_to=hive:" + hiveUser
		const sendAmount = "0.01"                     // DASH
		const expectDuffs int64 = 1_000_000 // 0.01 DASH * 1e8

		depositAddr, _, err := addrGen.GenerateDepositAddress(
			testPrimaryPubKeyHex, testBackupPubKeyHex, instruction,
		)
		if err != nil {
			t.Fatalf("derive deposit address: %v", err)
		}
		t.Logf("deposit address: %s (instruction=%q)", depositAddr, instruction)

		runDepositAndAssertCredit(ctx, t, d, addrGen, blockParser,
			depositAddr, instruction, hiveUser, sendAmount, expectDuffs,
			contractId,
		)
	})

	t.Run("Map_Idempotent_On_Retry", func(t *testing.T) {
		const hiveUser = "magi-3"
		const instruction = "deposit_to=hive:" + hiveUser

		depositAddr, _, err := addrGen.GenerateDepositAddress(
			testPrimaryPubKeyHex, testBackupPubKeyHex, instruction,
		)
		if err != nil {
			t.Fatalf("derive deposit address: %v", err)
		}

		// Confirm a deposit
		input := sendAndConfirmDeposit(ctx, t, d, blockParser, depositAddr, "0.005")
		// Submit map twice — second call should be a no-op (ISLockedMarker is "2"
		// after first call; HandleMap short-circuits).
		mapPayload := buildMapPayload(t, input, instruction)
		if _, err := d.CallContract(ctx, 1, contractId, "map", mapPayload); err != nil {
			t.Fatalf("first map: %v", err)
		}
		balAfterFirst, err := d.WaitForContractState(ctx, 2, contractId, "a-hive:"+hiveUser, 60*time.Second,
			func(v []byte) bool { return DecodeContractBalance(v) > 0 },
		)
		if err != nil {
			t.Fatalf("balance after first map: %v", err)
		}
		expected := int64(500_000)
		if got := DecodeContractBalance(balAfterFirst); got != expected {
			t.Fatalf("balance after first map: got %d duffs, want %d", got, expected)
		}

		// Second map call — must NOT double-credit.
		if _, err := d.CallContract(ctx, 1, contractId, "map", mapPayload); err != nil {
			t.Fatalf("second map (idempotency): %v", err)
		}
		// Give Hive a block for the second call to land.
		time.Sleep(8 * time.Second)
		balAfterSecond, err := d.QueryContractState(ctx, 2, contractId, "a-hive:"+hiveUser)
		if err != nil {
			t.Fatalf("balance after second map: %v", err)
		}
		if got := DecodeContractBalance(balAfterSecond); got != expected {
			t.Fatalf("idempotency FAIL: balance after retry %d duffs, want %d (no double credit)",
				got, expected)
		}
		t.Logf("idempotency OK: balance stayed at %d duffs across two map calls", expected)
	})

	t.Run("Multi_Deposits_Same_Block", func(t *testing.T) {
		// Two distinct deposit addresses, one tx each, all confirmed in the
		// same block. Proves the parser can find multiple matches in one
		// block and the contract handles multiple instructions per map call.
		const userA = "multi-a"
		const userB = "multi-b"
		instrA := "deposit_to=hive:" + userA
		instrB := "deposit_to=hive:" + userB

		addrA, _, err := addrGen.GenerateDepositAddress(testPrimaryPubKeyHex, testBackupPubKeyHex, instrA)
		if err != nil {
			t.Fatalf("derive addr A: %v", err)
		}
		addrB, _, err := addrGen.GenerateDepositAddress(testPrimaryPubKeyHex, testBackupPubKeyHex, instrB)
		if err != nil {
			t.Fatalf("derive addr B: %v", err)
		}

		// Send to both BEFORE mining so they confirm in the same block.
		const amtA = "0.004" // 400_000 duffs
		const amtB = "0.006" // 600_000 duffs
		txA, err := d.SendDashToAddress(ctx, addrA, amtA)
		if err != nil {
			t.Fatalf("sendto A: %v", err)
		}
		txB, err := d.SendDashToAddress(ctx, addrB, amtB)
		if err != nil {
			t.Fatalf("sendto B: %v", err)
		}
		t.Logf("queued multi-deposit: %s→%s (%s), %s→%s (%s)",
			txA, addrA, amtA, txB, addrB, amtB)

		// One block confirms both.
		newTip, err := d.MineDashBlocks(ctx, 1)
		if err != nil {
			t.Fatalf("mine: %v", err)
		}

		// addBlocks the new header.
		hdrHex, err := d.GetDashBlockHeaderHex(ctx, newTip)
		if err != nil {
			t.Fatalf("header at %d: %v", newTip, err)
		}
		addPayload := fmt.Sprintf(`{"blocks":"%s","latest_fee":1}`, hdrHex)
		if _, err := d.CallContract(ctx, 1, contractId, "addBlocks", addPayload); err != nil {
			t.Fatalf("addBlocks tip %d: %v", newTip, err)
		}

		// Parse the block — both deposits should be found.
		blockHash, err := d.GetDashBlockHashAtHeight(ctx, newTip)
		if err != nil {
			t.Fatalf("blockhash: %v", err)
		}
		rawBlockHex, err := d.GetDashRawBlockHex(ctx, blockHash)
		if err != nil {
			t.Fatalf("getblock: %v", err)
		}
		rawBlock, err := hex.DecodeString(rawBlockHex)
		if err != nil {
			t.Fatalf("decode raw block: %v", err)
		}
		inputs, err := blockParser.ParseBlock(rawBlock, []string{addrA, addrB}, newTip)
		if err != nil {
			t.Fatalf("ParseBlock: %v", err)
		}
		if len(inputs) != 2 {
			t.Fatalf("expected 2 inputs from block; got %d", len(inputs))
		}

		// Submit a map call for each input. Pass BOTH instructions so the
		// contract can resolve whichever output address each tx happens to
		// pay (we don't pre-sort inputs vs instructions).
		for i, in := range inputs {
			mapPayload, err := json.Marshal(struct {
				TxData struct {
					BlockHeight    uint32 `json:"block_height"`
					RawTxHex       string `json:"raw_tx_hex"`
					MerkleProofHex string `json:"merkle_proof_hex"`
					TxIndex        uint32 `json:"tx_index"`
				} `json:"tx_data"`
				Instructions []string `json:"instructions"`
			}{
				TxData: struct {
					BlockHeight    uint32 `json:"block_height"`
					RawTxHex       string `json:"raw_tx_hex"`
					MerkleProofHex string `json:"merkle_proof_hex"`
					TxIndex        uint32 `json:"tx_index"`
				}{
					BlockHeight:    in.BlockHeight,
					RawTxHex:       in.RawTxHex,
					MerkleProofHex: in.MerkleProofHex,
					TxIndex:        in.TxIndex,
				},
				Instructions: []string{instrA, instrB},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := d.CallContract(ctx, 1, contractId, "map", string(mapPayload)); err != nil {
				t.Fatalf("map call %d/%d: %v", i+1, len(inputs), err)
			}
		}

		// Verify both balances.
		balA, err := d.WaitForContractState(ctx, 2, contractId, "a-hive:"+userA, 90*time.Second,
			func(v []byte) bool { return DecodeContractBalance(v) > 0 },
		)
		if err != nil {
			t.Fatalf("balance A: %v", err)
		}
		balB, err := d.WaitForContractState(ctx, 2, contractId, "a-hive:"+userB, 90*time.Second,
			func(v []byte) bool { return DecodeContractBalance(v) > 0 },
		)
		if err != nil {
			t.Fatalf("balance B: %v", err)
		}
		if got := DecodeContractBalance(balA); got != 400_000 {
			t.Errorf("hive:%s expected 400_000 duffs, got %d", userA, got)
		}
		if got := DecodeContractBalance(balB); got != 600_000 {
			t.Errorf("hive:%s expected 600_000 duffs, got %d", userB, got)
		}
		t.Logf("multi-deposit OK: hive:%s=%d, hive:%s=%d",
			userA, DecodeContractBalance(balA),
			userB, DecodeContractBalance(balB))
	})

	t.Run("MapInstantSend_Then_Map_NoDoubleCredit", func(t *testing.T) {
		const hiveUser = "magi-4"
		const instruction = "deposit_to=hive:" + hiveUser

		depositAddr, _, err := addrGen.GenerateDepositAddress(
			testPrimaryPubKeyHex, testBackupPubKeyHex, instruction,
		)
		if err != nil {
			t.Fatalf("derive deposit address: %v", err)
		}

		// Send Dash but do NOT mine yet — emulate the IS-locked-mempool
		// state.
		txid, err := d.SendDashToAddress(ctx, depositAddr, "0.003")
		if err != nil {
			t.Fatalf("sendtoaddress: %v", err)
		}
		// The raw tx hex (mempool — getrawtransaction without -txindex needs
		// the mempool entry which exists immediately after send).
		rawTxHex, err := d.GetDashRawTransactionHex(ctx, txid, "")
		if err != nil {
			t.Fatalf("getrawtransaction (mempool): %v", err)
		}

		// Submit mapInstantSend — checkOracle accepts owner (magi-1) on
		// testnet builds.
		isPayload, err := json.Marshal(struct {
			RawTxHex     string   `json:"raw_tx_hex"`
			Instructions []string `json:"instructions"`
		}{
			RawTxHex:     rawTxHex,
			Instructions: []string{instruction},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.CallContract(ctx, 1, contractId, "mapInstantSend", string(isPayload)); err != nil {
			t.Fatalf("mapInstantSend: %v", err)
		}

		// Wait for the IS-locked credit to land.
		balAfterIS, err := d.WaitForContractState(ctx, 2, contractId, "a-hive:"+hiveUser, 60*time.Second,
			func(v []byte) bool { return DecodeContractBalance(v) > 0 },
		)
		if err != nil {
			t.Fatalf("balance after mapInstantSend: %v", err)
		}
		const expectDuffs int64 = 300_000
		if got := DecodeContractBalance(balAfterIS); got != expectDuffs {
			t.Fatalf("balance after IS credit: got %d duffs, want %d", got, expectDuffs)
		}
		t.Logf("mapInstantSend OK: %d duffs credited (no block yet)", expectDuffs)

		// Verify the il-<txid> marker was set to "1" (pending).
		marker, err := d.QueryContractState(ctx, 2, contractId, "il-"+txid)
		if err != nil {
			t.Fatalf("read il marker: %v", err)
		}
		if string(marker) != "1" {
			t.Fatalf("marker should be '1' (pending) after mapInstantSend; got %q", string(marker))
		}

		// Now confirm the tx in a block and submit the normal map(). It
		// must NOT double-credit and must upgrade the marker to "2".
		input := confirmAndExtract(ctx, t, d, blockParser, depositAddr, txid)
		mapPayload := buildMapPayload(t, input, instruction)
		if _, err := d.CallContract(ctx, 1, contractId, "map", mapPayload); err != nil {
			t.Fatalf("map after IS: %v", err)
		}
		// Allow the call to land. Marker should upgrade; balance unchanged.
		if _, err := d.WaitForContractState(ctx, 2, contractId, "il-"+txid, 60*time.Second,
			func(v []byte) bool { return string(v) == "2" },
		); err != nil {
			t.Fatalf("marker did not upgrade to '2': %v", err)
		}
		balAfterMap, err := d.QueryContractState(ctx, 2, contractId, "a-hive:"+hiveUser)
		if err != nil {
			t.Fatalf("balance after IS+map: %v", err)
		}
		if got := DecodeContractBalance(balAfterMap); got != expectDuffs {
			t.Fatalf("DOUBLE-CREDIT BUG: balance after IS+map %d duffs, want %d",
				got, expectDuffs)
		}
		t.Logf("IS+map idempotency OK: balance stayed at %d duffs, marker upgraded 1→2",
			expectDuffs)
	})

	// MUST be the last subtest — leaves the contract paused-then-unpaused
	// state, but to be safe we unpause at the end. Putting it last means a
	// stuck pause doesn't cascade-fail earlier subtests.
	t.Run("Pause_Rejects_Both_Map_Paths", func(t *testing.T) {
		// Pause the contract — owner-only action.
		if _, err := d.CallContract(ctx, 1, contractId, "pause", `""`); err != nil {
			t.Fatalf("pause: %v", err)
		}
		// Wait for the paused state to land on chain.
		if _, err := d.WaitForContractState(ctx, 2, contractId, "paused", 60*time.Second,
			func(v []byte) bool { return string(v) == "1" },
		); err != nil {
			t.Fatalf("paused state never set to '1': %v", err)
		}
		t.Log("contract paused; verifying both map paths reject")

		// ── (1) map path while paused ────────────────────────────────────
		const userMap = "paused-map-user"
		instrMap := "deposit_to=hive:" + userMap
		addrMap, _, err := addrGen.GenerateDepositAddress(
			testPrimaryPubKeyHex, testBackupPubKeyHex, instrMap,
		)
		if err != nil {
			t.Fatalf("derive paused-map addr: %v", err)
		}
		// Send + confirm + addBlocks (addBlocks is NOT pause-gated). Build
		// SPV proof and submit map() — call accepted by Hive but contract
		// must abort with "contract is paused", leaving the balance empty.
		input := sendAndConfirmDeposit(ctx, t, d, blockParser, addrMap, "0.002")
		mapPayload := buildMapPayload(t, input, instrMap)
		if _, err := d.CallContract(ctx, 1, contractId, "map", mapPayload); err != nil {
			t.Fatalf("map call (paused): %v", err)
		}
		// Give time for a Hive block to land. The contract should reject
		// internally — balance stays empty.
		time.Sleep(12 * time.Second)
		balMap, err := d.QueryContractState(ctx, 2, contractId, "a-hive:"+userMap)
		if err != nil {
			t.Fatalf("balance check (paused-map): %v", err)
		}
		if got := DecodeContractBalance(balMap); got > 0 {
			t.Fatalf("PAUSE-BYPASS: paused contract credited %d duffs via map", got)
		}
		t.Log("paused map: balance stayed at 0 ✓")

		// ── (2) mapInstantSend path while paused ─────────────────────────
		const userIS = "paused-is-user"
		instrIS := "deposit_to=hive:" + userIS
		addrIS, _, err := addrGen.GenerateDepositAddress(
			testPrimaryPubKeyHex, testBackupPubKeyHex, instrIS,
		)
		if err != nil {
			t.Fatalf("derive paused-IS addr: %v", err)
		}
		txid, err := d.SendDashToAddress(ctx, addrIS, "0.003")
		if err != nil {
			t.Fatalf("send paused-IS: %v", err)
		}
		rawTxHex, err := d.GetDashRawTransactionHex(ctx, txid, "")
		if err != nil {
			t.Fatalf("getrawtransaction (paused-IS): %v", err)
		}
		isPayload, err := json.Marshal(struct {
			RawTxHex     string   `json:"raw_tx_hex"`
			Instructions []string `json:"instructions"`
		}{
			RawTxHex:     rawTxHex,
			Instructions: []string{instrIS},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.CallContract(ctx, 1, contractId, "mapInstantSend", string(isPayload)); err != nil {
			t.Fatalf("mapInstantSend call (paused): %v", err)
		}
		time.Sleep(12 * time.Second)
		balIS, err := d.QueryContractState(ctx, 2, contractId, "a-hive:"+userIS)
		if err != nil {
			t.Fatalf("balance check (paused-IS): %v", err)
		}
		if got := DecodeContractBalance(balIS); got > 0 {
			t.Fatalf("PAUSE-BYPASS: paused contract credited %d duffs via mapInstantSend", got)
		}
		t.Log("paused mapInstantSend: balance stayed at 0 ✓")

		// ── Unpause and verify the gate flips back ────────────────────────
		if _, err := d.CallContract(ctx, 1, contractId, "unpause", `""`); err != nil {
			t.Fatalf("unpause: %v", err)
		}
		if _, err := d.WaitForContractState(ctx, 2, contractId, "paused", 60*time.Second,
			func(v []byte) bool { return string(v) != "1" },
		); err != nil {
			t.Fatalf("paused state never cleared: %v", err)
		}
		t.Log("contract unpaused; pause-gate cycle complete")
	})
}

// ── Test helpers (top-level, reused by subtests) ────────────────────────

// sendAndConfirmDeposit sends `amount` DASH to `depositAddr` from the
// regtest wallet, mines a confirmation block, addBlocks-relays the header
// onto the contract, and returns the parsed SPV proof for that tx.
func sendAndConfirmDeposit(
	ctx context.Context, t *testing.T, d *Devnet,
	parser *chain.BTCBlockParser, depositAddr, amount string,
) chain.MappingInput {
	t.Helper()
	txid, err := d.SendDashToAddress(ctx, depositAddr, amount)
	if err != nil {
		t.Fatalf("sendtoaddress: %v", err)
	}
	t.Logf("send tx: %s (%s DASH)", txid, amount)
	return confirmAndExtract(ctx, t, d, parser, depositAddr, txid)
}

// confirmAndExtract mines a block containing `txid`, relays the header
// via addBlocks, then parses the block to extract the deposit's SPV
// proof. Assumes the tx is already in the mempool.
func confirmAndExtract(
	ctx context.Context, t *testing.T, d *Devnet,
	parser *chain.BTCBlockParser, depositAddr, txid string,
) chain.MappingInput {
	t.Helper()
	newTip, err := d.MineDashBlocks(ctx, 1)
	if err != nil {
		t.Fatalf("mine confirmation: %v", err)
	}

	hdrHex, err := d.GetDashBlockHeaderHex(ctx, newTip)
	if err != nil {
		t.Fatalf("header at %d: %v", newTip, err)
	}
	// addBlocks: oracle-gated; owner-bypass accepts magi-1 on regtest builds.
	addPayload := fmt.Sprintf(`{"blocks":"%s","latest_fee":1}`, hdrHex)
	if _, err := getContractDeployer(d).CallContract(ctx, 1, contractIdFromTest(t), "addBlocks", addPayload); err != nil {
		t.Fatalf("addBlocks for tip %d: %v", newTip, err)
	}

	blockHash, err := d.GetDashBlockHashAtHeight(ctx, newTip)
	if err != nil {
		t.Fatalf("blockhash at %d: %v", newTip, err)
	}
	rawBlockHex, err := d.GetDashRawBlockHex(ctx, blockHash)
	if err != nil {
		t.Fatalf("getblock %s: %v", blockHash, err)
	}
	rawBlock, err := hex.DecodeString(rawBlockHex)
	if err != nil {
		t.Fatalf("decode raw block: %v", err)
	}
	inputs, err := parser.ParseBlock(rawBlock, []string{depositAddr}, newTip)
	if err != nil {
		t.Fatalf("ParseBlock: %v", err)
	}
	for _, in := range inputs {
		// Match by reconstructing the txid from rawtx.
		if in.RawTxHex == "" {
			continue
		}
		// We don't have a direct txid in MappingInput; just use the first
		// matched output. In tests with a single deposit per block this is
		// safe.
		return in
	}
	t.Fatalf("no deposit found in block %d for address %s", newTip, depositAddr)
	return chain.MappingInput{}
}

// buildMapPayload serializes a MappingInput + instruction into the JSON
// shape the contract's `map` action expects.
func buildMapPayload(t *testing.T, input chain.MappingInput, instruction string) string {
	t.Helper()
	payload, err := json.Marshal(struct {
		TxData struct {
			BlockHeight    uint32 `json:"block_height"`
			RawTxHex       string `json:"raw_tx_hex"`
			MerkleProofHex string `json:"merkle_proof_hex"`
			TxIndex        uint32 `json:"tx_index"`
		} `json:"tx_data"`
		Instructions []string `json:"instructions"`
	}{
		TxData: struct {
			BlockHeight    uint32 `json:"block_height"`
			RawTxHex       string `json:"raw_tx_hex"`
			MerkleProofHex string `json:"merkle_proof_hex"`
			TxIndex        uint32 `json:"tx_index"`
		}{
			BlockHeight:    input.BlockHeight,
			RawTxHex:       input.RawTxHex,
			MerkleProofHex: input.MerkleProofHex,
			TxIndex:        input.TxIndex,
		},
		Instructions: []string{instruction},
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}

// runDepositAndAssertCredit executes the happy-path deposit cycle and
// asserts the expected duff amount lands at a-hive:<user>.
func runDepositAndAssertCredit(
	ctx context.Context, t *testing.T, d *Devnet,
	addrGen *chain.BTCAddressGenerator,
	parser *chain.BTCBlockParser,
	depositAddr, instruction, hiveUser, sendAmount string,
	expectDuffs int64,
	contractId string,
) {
	t.Helper()
	input := sendAndConfirmDeposit(ctx, t, d, parser, depositAddr, sendAmount)
	mapPayload := buildMapPayload(t, input, instruction)
	if _, err := d.CallContract(ctx, 1, contractId, "map", mapPayload); err != nil {
		t.Fatalf("map: %v", err)
	}
	bal, err := d.WaitForContractState(ctx, 2, contractId, "a-hive:"+hiveUser, 90*time.Second,
		func(v []byte) bool { return DecodeContractBalance(v) > 0 },
	)
	if err != nil {
		t.Fatalf("balance never landed for hive:%s: %v", hiveUser, err)
	}
	if got := DecodeContractBalance(bal); got != expectDuffs {
		t.Fatalf("credit mismatch for hive:%s — got %d duffs, want %d",
			hiveUser, got, expectDuffs)
	}
	t.Logf("PASS: hive:%s credited %d duffs", hiveUser, expectDuffs)
}

// ── plumbing kludges (test-local; sharing the same contractId in helpers
// without ferrying it through every function param) ─────────────────────

// contractIdFromTest fetches the contract id stashed in the parent test's
// closure via a thread-local. To avoid singleton state and keep these
// helpers simple, we look it up from the parent t.Name() — but that's
// fragile, so for now subtests pass the contractId themselves where
// needed. confirmAndExtract relies on the addBlocks call working with
// magi-1 as owner — the addBlocks payload is the same regardless of which
// contract id we pass, so we use a package-local last-deployed cache.
var lastDeployedContractId string

func contractIdFromTest(_ *testing.T) string {
	return lastDeployedContractId
}

// getContractDeployer is here so confirmAndExtract can perform CallContract
// without needing a Devnet pointer in its signature. Currently a passthrough
// — kept as a seam in case we want to centralize retry/back-off later.
func getContractDeployer(d *Devnet) *Devnet { return d }
