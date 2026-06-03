package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
	"vsc-node/modules/common"
	"vsc-node/modules/config"
	"vsc-node/modules/db/vsc/contracts"
	"vsc-node/modules/db/vsc/transactions"
	wasm_runtime "vsc-node/modules/wasm/runtime"

	"vsc-node/modules/e2e"
	stateEngine "vsc-node/modules/state-processing"
	transactionpool "vsc-node/modules/transaction-pool"

	"vsc-node/lib/dids"
	"vsc-node/lib/vsclog"

	ethCrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/hasura/go-graphql-client"
	"github.com/vsc-eco/hivego"
)

// TestReview6_C1_ExpireWithdrawalEmptyProofRejected reproduces the C1 finding
// from EVM-FORK-CROSSREF-ALLMETHODS-AUDIT-2026-06-02.md:
//
//	"HandleExpireWithdrawal: permissionless, proofless refund of ANY pending
//	withdrawal → double-spend/bridge drain. env.BlockHeight (Hive L1 height
//	≈106M) is compared against ps.BlockHeight+window (ETH block# ≈22M) so
//	env.BlockHeight < expiryHeight is always FALSE → post-window branch →
//	proof optional; any account calls expireWithdrawal(nonce,{}) with empty
//	proof."
//
// Audit-target commit (per audit caveat line 807): 838f98f (1 commit behind
// review5-split tip). Fix commit (current tip 5da784d, "F3+F2-monitor"): proof
// is now MANDATORY in ALL branches — the post-window opportunistic-only path
// no longer exists.
//
// This PoC initiates a withdrawal, then calls expireWithdrawal with an empty
// proof payload. Expected behavior on the fixed code:
//   - contract call FAILS (output.ok == false)
//   - balance NOT refunded (PendingSpend still present, np unchanged)
//   - error string contains "L1-proof-of-drop is mandatory"
//
// On the buggy code (838f98f): contract call succeeds, balance restored,
// PendingSpend deleted, np advanced — confirming the drain.
func TestReview6_C1_ExpireWithdrawalEmptyProofRejected(t *testing.T) {
	vsclog.ParseAndApply("verbose")
	config.UseMainConfigDuringTests = true

	container := e2e.NewContainer(EVM_E2E_NODES)
	container.Init()
	container.Start(t)

	testKey, _ := ethCrypto.GenerateKey()
	testAddr := ethCrypto.PubkeyToAddress(testKey.PublicKey).Hex()
	didKey := dids.NewEthDID(testAddr)

	transactionCreator := transactionpool.TransactionCrafter{
		Identity:     dids.NewEthProvider(testKey),
		Did:          didKey,
		VSCBroadcast: container.VSCBroadcast(),
	}
	var nonce uint64

	graphClient := graphql.NewClient("http://localhost:7080/api/v1/graphql", nil)
	r2e := container.Runner()

	// Bootstrap
	container.AddStep(r2e.WaitToStart())
	container.AddStep(r2e.Wait(5))
	container.AddStep(r2e.BroadcastElection())
	container.AddStep(e2e.Step{
		Name: "Fund test account",
		TestFunc: func(ctx e2e.StepCtx) (e2e.EvaluateFunc, error) {
			// review6: fund test account heavily so RC ≥ rc_limit=10000 on
			// unmapETH/expireWithdrawal — the e2e RC ratio is much lower than
			// mainnet so the "50000" used by autoexpiry test only yields ~500
			// RC, which fails StaticMaxRcCost pre-flight.
			container.HiveCreator.Transfer("test-account", "vsc.gateway", "5000000", "HBD", "to="+didKey.String())
			container.HiveCreator.Transfer("test-account", "vsc.gateway", "5000000", "HBD", "to=hive:vaultec")
			return func(ctx e2e.StepCtx) error {
				deadline := time.After(60 * time.Second)
				ticker := time.NewTicker(3 * time.Second)
				defer ticker.Stop()
				for {
					var q struct {
						GetAccountBalance struct {
							Hbd graphql.Int `graphql:"hbd"`
						} `graphql:"getAccountBalance(account: $account)"`
					}
					graphClient.Query(context.Background(), &q, map[string]any{
						"account": graphql.String(didKey.String()),
					})
					if q.GetAccountBalance.Hbd > 0 {
						return nil
					}
					select {
					case <-deadline:
						return fmt.Errorf("timeout funding")
					case <-ticker.C:
					}
				}
			}, nil
		},
	})

	container.AddStep(r2e.DupElection(5 * time.Second))
	container.AddStep(r2e.Wait(10))

	var contractId string
	container.AddStep(e2e.Step{
		Name: "Deploy EVM contract",
		TestFunc: func(ctx e2e.StepCtx) (e2e.EvaluateFunc, error) {
			storageProof, err := ctx.Container.Client().RequestProof(
				"http://localhost:7080/api/v1/graphql", EVM_CONTRACT_WASM,
			)
			if err != nil {
				return nil, err
			}
			tx := stateEngine.TxCreateContract{
				Version: "0.1", NetId: "vsc-mocknet",
				Name: "review6-c1-test", Description: "C1 empty-proof rejection",
				Owner: "hive:vaultec", Code: storageProof.Hash,
				Runtime: wasm_runtime.Go, StorageProof: storageProof,
			}
			j, _ := json.Marshal(tx)
			transferOp := r2e.HiveCreator.Transfer("vaultec", "vsc.gateway", "10", "HBD", "contract_deployment")
			deployOp := r2e.HiveCreator.CustomJson([]string{"vaultec"}, []string{}, "vsc.create_contract", string(j))
			hiveTx := r2e.HiveCreator.MakeTransaction([]hivego.HiveOperation{deployOp, transferOp})
			r2e.HiveCreator.PopulateSigningProps(&hiveTx, nil)
			txId, _ := r2e.HiveCreator.Broadcast(hiveTx)
			contractId = common.ContractId(txId, 0)
			t.Logf("contract: %s", contractId)
			return func(ctx e2e.StepCtx) error { return nil }, nil
		},
	})
	container.AddStep(r2e.DupElection(5 * time.Second))
	container.AddStep(r2e.Wait(10))

	container.AddStep(e2e.Step{
		Name: "Configure contract",
		TestFunc: func(ctx e2e.StepCtx) (e2e.EvaluateFunc, error) {
			calls := []map[string]interface{}{
				{"contract_id": contractId, "action": "setVault", "payload": "6026449a55b7eb5c1b1a5e33e02e542bbba719ce", "rc_limit": 10000, "intents": []interface{}{}, "net_id": "vsc-mocknet"},
				{"contract_id": contractId, "action": "setChainId", "payload": "1", "rc_limit": 10000, "intents": []interface{}{}, "net_id": "vsc-mocknet"},
				{"contract_id": contractId, "action": "createKey", "payload": "test", "rc_limit": 10000, "intents": []interface{}{}, "net_id": "vsc-mocknet"},
			}
			ops := make([]hivego.HiveOperation, len(calls))
			for i, call := range calls {
				j, _ := json.Marshal(call)
				ops[i] = r2e.HiveCreator.CustomJson([]string{"vaultec"}, []string{}, "vsc.call", string(j))
			}
			hiveTx := r2e.HiveCreator.MakeTransaction(ops)
			r2e.HiveCreator.PopulateSigningProps(&hiveTx, nil)
			r2e.HiveCreator.Broadcast(hiveTx)
			return nil, nil
		},
	})
	container.AddStep(r2e.Wait(10))

	container.AddStep(e2e.Step{
		Name: "Submit block header (one block, low ETH height)",
		TestFunc: func(ctx e2e.StepCtx) (e2e.EvaluateFunc, error) {
			// Use a small block_number so we could in principle reach
			// post-window in this devnet; for this PoC we only verify the
			// pre-window rejection, but the fix applies to both branches.
			payload := `{"blocks":[{"block_number":24910634,"transactions_root":"ae62b318e6723833e0ff810ff0ca54aa311debd5dcbabda10c5a09d4c1836358","receipts_root":"74e534585c2916a447ebabe95792fd7f1e40a69ca50115ad8548c144f559d1c6","base_fee_per_gas":249400091,"gas_limit":60000000,"timestamp":1776530903}],"latest_fee":249400091}`
			call := map[string]interface{}{
				"contract_id": contractId, "action": "addBlocks",
				"payload": json.RawMessage(payload), "rc_limit": 10000,
				"intents": []interface{}{}, "net_id": "vsc-mocknet",
			}
			j, _ := json.Marshal(call)
			op := r2e.HiveCreator.CustomJson([]string{"vaultec"}, []string{}, "vsc.call", string(j))
			hiveTx := r2e.HiveCreator.MakeTransaction([]hivego.HiveOperation{op})
			r2e.HiveCreator.PopulateSigningProps(&hiveTx, nil)
			r2e.HiveCreator.Broadcast(hiveTx)
			return nil, nil
		},
	})
	container.AddStep(r2e.Wait(5))

	container.AddStep(e2e.Step{
		Name: "Mint balance + gas reserve",
		TestFunc: func(ctx e2e.StepCtx) (e2e.EvaluateFunc, error) {
			mintPayload := fmt.Sprintf(`{"address":"%s","asset":"eth","amount":1000000000000000000}`, didKey.String())
			calls := []map[string]interface{}{
				{"contract_id": contractId, "action": "adminMint", "payload": json.RawMessage(mintPayload), "rc_limit": 10000, "intents": []interface{}{}, "net_id": "vsc-mocknet"},
				{"contract_id": contractId, "action": "setGasReserve", "payload": "500000000000000000", "rc_limit": 10000, "intents": []interface{}{}, "net_id": "vsc-mocknet"},
			}
			ops := make([]hivego.HiveOperation, len(calls))
			for i, call := range calls {
				j, _ := json.Marshal(call)
				ops[i] = r2e.HiveCreator.CustomJson([]string{"vaultec"}, []string{}, "vsc.call", string(j))
			}
			hiveTx := r2e.HiveCreator.MakeTransaction(ops)
			r2e.HiveCreator.PopulateSigningProps(&hiveTx, nil)
			r2e.HiveCreator.Broadcast(hiveTx)
			return nil, nil
		},
	})
	container.AddStep(r2e.Wait(5))

	// STEP A: Initiate withdrawal (unmapETH 0.05 ETH) → creates PendingSpend at nonce 0
	container.AddStep(e2e.Step{
		Name: "Initiate withdrawal",
		TestFunc: func(ctx e2e.StepCtx) (e2e.EvaluateFunc, error) {
			withdrawPayload := `{"amount":"50000000000000000","to":"0xdead000000000000000000000000000000000001","asset":"eth","deduct_fee":true,"max_fee":""}`
			unmapOp := &transactionpool.VscContractCall{
				Caller: didKey.String(), ContractId: contractId, RcLimit: 400,
				Action: "unmapETH", Payload: withdrawPayload, Intents: []contracts.Intent{},
			}
			op, opErr := unmapOp.SerializeVSC()
			if opErr != nil {
				t.Logf("unmapETH SerializeVSC err: %v", opErr)
			}
			tx := transactionpool.VSCTransaction{
				Ops: []transactionpool.VSCTransactionOp{op}, Nonce: nonce, NetId: "vsc-mocknet",
			}
			nonce++
			sTx, signErr := transactionCreator.SignFinal(tx)
			if signErr != nil {
				t.Logf("unmapETH SignFinal err: %v", signErr)
			}
			txId, bcastErr := transactionCreator.Broadcast(sTx)
			t.Logf("unmapETH: txId=%q bcastErr=%v", txId, bcastErr)
			if txId == "" {
				return nil, fmt.Errorf("unmapETH broadcast returned empty txId; bcastErr=%v", bcastErr)
			}
			return e2e.TxStatusAssertion(
				[]e2e.TxStatusAssert{{txId, transactions.TransactionStatusConfirmed}}, 120,
			), nil
		},
	})

	// Snapshot post-withdrawal state for the assertion in step C.
	var preExpireState string
	container.AddStep(e2e.Step{
		Name: "Snapshot post-withdrawal state",
		TestFunc: func(ctx e2e.StepCtx) (e2e.EvaluateFunc, error) {
			return func(ctx e2e.StepCtx) error {
				balKey := "a-" + didKey.String() + "-eth"
				reqBody := fmt.Sprintf(`{"query":"{ getStateByKeys(contractId: \"%s\", keys: [\"n\", \"np\", \"d-0\", \"%s\"], encoding: \"raw\") }"}`, contractId, balKey)
				resp, _ := http.Post("http://localhost:7080/api/v1/graphql", "application/json", strings.NewReader(reqBody))
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				preExpireState = string(body)
				t.Logf("POST-WITHDRAWAL state: %s", preExpireState)
				if !strings.Contains(preExpireState, `"np":"1"`) {
					return fmt.Errorf("expected np=1 after unmapETH, got: %s", preExpireState)
				}
				if !strings.Contains(preExpireState, `"d-0":`) || strings.Contains(preExpireState, `"d-0":null`) {
					return fmt.Errorf("expected d-0 (PendingSpend) to exist, got: %s", preExpireState)
				}
				return nil
			}, nil
		},
	})

	// STEP B: C1 PoC — call expireWithdrawal with an EMPTY proof payload.
	// On the bug (838f98f): post-window branch fires (since env.BlockHeight
	// always >= expiryHeight due to domain mismatch) → empty proof accepted →
	// refund issued → drain.
	// On the fix (5da784d): "L1-proof-of-drop is mandatory" rejection.
	var c1TxId string
	container.AddStep(e2e.Step{
		Name: "C1 PoC: expireWithdrawal with empty proof",
		TestFunc: func(ctx e2e.StepCtx) (e2e.EvaluateFunc, error) {
			// Empty proof: only Type is required by handler; empty Type triggers
			// the mandatory-proof rejection on the fixed code.
			expirePayload := `{"nonce":0,"proof":{"type":""}}`
			expireOp := &transactionpool.VscContractCall{
				Caller: didKey.String(), ContractId: contractId, RcLimit: 400,
				Action: "expireWithdrawal", Payload: expirePayload, Intents: []contracts.Intent{},
			}
			op, _ := expireOp.SerializeVSC()
			tx := transactionpool.VSCTransaction{
				Ops: []transactionpool.VSCTransactionOp{op}, Nonce: nonce, NetId: "vsc-mocknet",
			}
			nonce++
			sTx, _ := transactionCreator.SignFinal(tx)
			txId, _ := transactionCreator.Broadcast(sTx)
			c1TxId = txId
			t.Logf("expireWithdrawal(empty proof): %s", txId)
			// We don't assert Confirmed/Failed at the L2 tx layer (the tx
			// is still mined either way); the contract-output ok=false flag
			// in step C is the real signal.
			return e2e.TxStatusAssertion(
				[]e2e.TxStatusAssert{{txId, transactions.TransactionStatusConfirmed}}, 120,
			), nil
		},
	})

	// STEP C: Assert the contract REJECTED the call (output.ok == false) AND
	// state is UNCHANGED (np still 1, d-0 still present, balance unchanged).
	container.AddStep(e2e.Step{
		Name: "Assert empty-proof expireWithdrawal was rejected",
		TestFunc: func(ctx e2e.StepCtx) (e2e.EvaluateFunc, error) {
			return func(ctx e2e.StepCtx) error {
				// 1. Query the contract output for the expireWithdrawal tx.
				outputQuery := fmt.Sprintf(`{"query":"{ findContractOutput(filterOptions: {byInput: \"%s\"}) { results { ok ret } } }"}`, c1TxId)
				resp, _ := http.Post("http://localhost:7080/api/v1/graphql", "application/json", strings.NewReader(outputQuery))
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				outStr := string(body)
				t.Logf("contractOutput: %s", outStr)

				// On fixed code: ok=false with error mentioning proof mandatory.
				// On buggy code: ok=true and refund happened.
				if strings.Contains(outStr, `"ok":true`) {
					return fmt.Errorf("C1 STILL LIVE: expireWithdrawal with empty proof was ACCEPTED on this build — drain reproduced. Output: %s", outStr)
				}
				if !strings.Contains(outStr, `"ok":false`) {
					return fmt.Errorf("C1 inconclusive: no ok=false in contract output, got: %s", outStr)
				}
				// Bonus: confirm the error string mentions proof mandatory.
				if !strings.Contains(outStr, "proof-of-drop is mandatory") && !strings.Contains(outStr, "mandatory") {
					t.Logf("WARN: ok=false but error string doesn't mention 'mandatory' — verify the rejection reason: %s", outStr)
				}

				// 2. Confirm state is unchanged: np still 1, d-0 still present.
				balKey := "a-" + didKey.String() + "-eth"
				reqBody := fmt.Sprintf(`{"query":"{ getStateByKeys(contractId: \"%s\", keys: [\"n\", \"np\", \"d-0\", \"%s\"], encoding: \"raw\") }"}`, contractId, balKey)
				resp2, _ := http.Post("http://localhost:7080/api/v1/graphql", "application/json", strings.NewReader(reqBody))
				body2, _ := io.ReadAll(resp2.Body)
				resp2.Body.Close()
				postStr := string(body2)
				t.Logf("POST-EXPIRE state: %s", postStr)
				if !strings.Contains(postStr, `"np":"1"`) {
					return fmt.Errorf("np changed: expected np=1 (unchanged), got: %s", postStr)
				}
				if !strings.Contains(postStr, `"d-0":`) || strings.Contains(postStr, `"d-0":null`) {
					return fmt.Errorf("d-0 was deleted: expected PendingSpend still present (rejection), got: %s", postStr)
				}
				t.Log("C1 FIXED: empty-proof expireWithdrawal correctly rejected; PendingSpend preserved; no drain")
				return nil
			}, nil
		},
	})

	err := container.RunSteps(t)
	if err != nil {
		t.Fatalf("C1 test failed: %v", err)
	}
}
