package devnet

// RC-TREADMILL-1 on a real network: an account whose whole RC budget is the
// Hive free allowance spends it, then keeps retrying — before and after the
// floor reaches consensus 0.18.0.
//
// Below the line every retry is charged the flat 100 RC of a refused call
// although the account has none, the clamp pins the stored total at the
// account's ceiling, and the write re-anchors the RC_RETURN_PERIOD schedule for
// the whole total: 10,000 RC returning at 0.069 RC a block is truncated away by
// any re-anchor under 15 blocks apart, so the account never gets a single RC
// back for as long as it keeps trying. From 0.18.0 a payer that cannot cover a
// charge is not charged, the slot leaves its record alone, and the return runs.
// Once the account has the 100 RC a refused call costs it is charged again —
// the rule is "all of the charge or none of it", not "failures are free".
//
// The measurement is the account's own RC record: how far its anchor sits
// behind the chain head is how much of its return has survived. Below the line
// the anchor is dragged along by every retry and stays at the head; from 0.18.0
// it stays where it was and falls behind.
//
// Every retry is a Hive-anchored vsc.call: the transaction pool's RC gate is
// for offchain transactions only, so an L1 call from an account with no RC is
// always included, always refused, and was always charged.
//
// Available RC is read the way a wallet reads it (getAccountRC), which is also
// the live check on RC-GQL-2: an account with no balance record used to be
// reported as holding its whole allowance however much of it was frozen.
//
//	DEVNET_FASTBLOCKS=1 DEVNET_BUDGET_CAP=150m \
//	  go test -v -run TestRcTreadmill_Activation -timeout 180m ./tests/devnet/
//
// The last case waits out the ~1,440 blocks the free allowance takes to return
// the 100 RC a call costs, which is 72 minutes of the test clock whatever the
// real cadence — hence the raised budget cap. Without it that case records
// itself as skipped; what the rule does is covered either way by the
// funded-payer case before it.

import (
	"fmt"
	"testing"
	"time"

	"vsc-node/modules/common/params"
	systemconfig "vsc-node/modules/common/system-config"
)

const (
	// rctRetryLimit is the rc_limit each retry declares. It is never spent: the
	// engine refuses the call for want of RC before it runs, and charges the
	// flat 100 RC of a refused call — the charge this test is about.
	rctRetryLimit = 1000
	// rctRetries is how many refused calls the account makes in each phase.
	// Each spans a slot or two, so a phase covers far more than the 15 blocks
	// one RC of the free allowance takes to return.
	rctRetries = 12
	// rctCallMinRc is the RC a call is charged at the least, and so also the RC
	// a payer must have for the engine to run one at all (params.MINIMUM_RC_LIMIT
	// is the floor on the limit; this is the floor on the charge).
	rctCallMinRc = 100
	// rctReturnBlocksPerRc is how many blocks return one RC of the free
	// allowance: RC_RETURN_PERIOD / RC_HIVE_FREE_AMOUNT = 144,000 / 10,000.
	rctReturnBlocksPerRc = 15
)

func TestRcTreadmill_Activation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping devnet RC treadmill test in short mode")
	}
	requireDocker(t)

	wasm := buildRcProbeContract(t)

	const nodes = 5
	cfg := DefaultConfig()
	cfg.Nodes = nodes
	cfg.LogLevel = "info"
	cfg.SysConfigOverrides = &systemconfig.SysConfigOverrides{
		ConsensusParams: &params.ConsensusParams{
			ElectionInterval:               20,
			ConsensusVersionFloorEpoch:     1,
			ConsensusVersionFloorMajor:     0,
			ConsensusVersionFloorConsensus: 17,
		},
	}
	d, ctx := startDevnetNoKey(t, cfg, vfTestBudget(140*time.Minute))

	if err := d.WaitForBlockProcessing(ctx, 1, 30, 8*time.Minute); err != nil {
		dumpDiagnostics(t, d, ctx)
		t.Fatalf("network never reached block 30: %v", err)
	}
	if err := d.waitForElectionEpoch(ctx, 1, 1, 10*time.Minute); err != nil {
		t.Fatalf("first election never indexed: %v", err)
	}
	if err := d.waitForRcBackfill(ctx); err != nil {
		t.Fatalf("witness RC funding: %v", err)
	}
	cid, err := d.DeployContract(ctx, ContractDeployOpts{WasmPath: wasm, Name: "rcprobe", DeployerNode: 1, GQLNode: 2})
	if err != nil {
		t.Fatalf("deploy the probe contract: %v", err)
	}
	t.Logf("probe contract %s", cid)
	if err := d.WaitForBlockProcessing(ctx, 2, nodeBlock(t, d, ctx, 1), 3*time.Minute); err != nil {
		t.Fatalf("nodes not synced after the deploy: %v", err)
	}
	rcpSettleAfterDeploy(t, d, ctx, nodes)

	// initminer holds no HBD on the Magi ledger: its whole RC budget is the
	// free allowance every Hive account gets.
	const freeTier = "initminer"
	const payer = "hive:" + freeTier
	allNodes := []int{1, 2, 3, 4, 5}

	// rcNow is the account's available RC and its ceiling, as a wallet reads them.
	rcNow := func(t *testing.T) (amount, max int64) {
		t.Helper()
		var rc struct {
			R struct {
				Amount int64 `json:"amount"`
				Max    int64 `json:"max_rcs"`
			} `json:"getAccountRC"`
		}
		if err := d.gqlQuery(ctx, rcpRefNode, `query($a:String!){getAccountRC(account:$a){amount max_rcs}}`,
			map[string]any{"a": payer}, &rc); err != nil {
			t.Fatalf("reading %s RC: %v", payer, err)
		}
		return rc.R.Amount, rc.R.Max
	}
	// records is every RC record the reference node holds for the account.
	records := func(t *testing.T) []string {
		t.Helper()
		recs, err := rcpAll(ctx, d, rcpRefNode, payer)
		if err != nil {
			t.Fatalf("reading %s RC records: %v", payer, err)
		}
		return recs
	}
	// anchor is the height of the account's newest RC record: the height its
	// return is measured from.
	anchor := func(t *testing.T) uint64 {
		t.Helper()
		rec, ok, err := rcpLatest(ctx, d, rcpRefNode, payer)
		if err != nil {
			t.Fatalf("reading %s RC record: %v", payer, err)
		}
		if !ok {
			return 0
		}
		return rec.Height
	}
	// send broadcasts one Hive-anchored call and returns its final status.
	send := func(t *testing.T, tag string) string {
		t.Helper()
		txId, err := rcpCall(d, freeTier, cid, "noop", "x", rctRetryLimit, nil)
		if err != nil {
			t.Fatalf("%s: broadcasting: %v", tag, err)
		}
		status := rcpWaitFinal(d, ctx, rcpRefNode, txId, 8*time.Minute)
		if status == "" {
			t.Fatalf("%s: never became final (tx %s)", tag, txId)
		}
		return status
	}
	// retry is a call the engine must refuse: the account cannot pay the 100 RC
	// a call is charged at the least, so it never runs.
	retry := func(t *testing.T, tag string) {
		t.Helper()
		if status := send(t, tag); status != "FAILED" {
			t.Fatalf("%s: status %s, want FAILED — an account that cannot pay the %d RC minimum cannot have "+
				"its call run", tag, status, rctCallMinRc)
		}
	}
	// drain leaves the account unable to pay for a call, and checks it was
	// charged for the privilege — which holds at either consensus line, because
	// an account that CAN pay is always charged.
	//
	// It takes two calls. A call's gas budget is min(available, rc_limit) where
	// `available` is what CanConsume(payer, h, 100) reports left AFTER the 100 RC
	// minimum, so a call that runs out is charged everything but those 100 and
	// one more call is needed to take them.
	drain := func(t *testing.T, tag string) {
		t.Helper()
		avail, _ := rcNow(t)
		if avail > rctCallMinRc {
			o := rcpMeasure(t, d, ctx, freeTier, cid, "logBlob", "50000", int(avail), nil)
			if o.Status != "FAILED" {
				t.Fatalf("%s: a 50,000 byte log under rc_limit %d: status %s, want FAILED", tag, avail, o.Status)
			}
			if o.Used < avail-rctCallMinRc-100 {
				t.Fatalf("%s: the run-out call was charged %d RC of the %d available, want all but the %d "+
					"minimum the gate holds back", tag, o.Used, avail, rctCallMinRc)
			}
			t.Logf("%s: the run-out call was charged %d of %d RC", tag, o.Used, avail)
			avail, _ = rcNow(t)
		}
		if avail >= rctCallMinRc {
			prev, havePrev, err := rcpLatest(ctx, d, rcpRefNode, payer)
			if err != nil {
				t.Fatalf("%s: reading the RC record: %v", tag, err)
			}
			// This one is affordable by construction, so it is not refused: the
			// gate passes on exactly the minimum, the call runs on a gas budget
			// of nothing, and a trivial action still finishes. Either way it is
			// charged the minimum, which is the point.
			status := send(t, tag+": the last call it can pay for")
			charged := rcpWaitCharge(t, d, ctx, payer, prev, havePrev)
			if charged != rctCallMinRc {
				t.Fatalf("%s: a call with %d RC available was charged %d, want the %d minimum",
					tag, avail, charged, rctCallMinRc)
			}
			t.Logf("%s: the last call it could pay for was charged %d RC (status %s)", tag, charged, status)
		}
		left, _ := rcNow(t)
		if left >= rctCallMinRc {
			t.Fatalf("%s: %d RC still available, want the account unable to pay the %d a call costs",
				tag, left, rctCallMinRc)
		}
		t.Logf("%s: %d RC available, records %v", tag, left, records(t))
	}
	must := func(name string, f func(t *testing.T)) {
		t.Helper()
		if !t.Run(name, f) {
			dumpDiagnostics(t, d, ctx)
			t.Fatalf("%s failed; the cases after it depend on it", name)
		}
	}

	// ------------------------------------------------------------------
	// Before: the chain runs consensus 0.17.
	// ------------------------------------------------------------------
	must("before/01_every_node_runs_0.18_code_on_an_active_0.17", func(t *testing.T) {
		if err := pollAllNodes(nodes, 8*time.Minute, func(n int) bool {
			line, running, _, e := d.ConsensusInfo(ctx, n)
			return e == nil && line == "0.17" && cvRunsAtLeast(running, 18)
		}); err != nil {
			line, running, _, _ := d.ConsensusInfo(ctx, 1)
			t.Fatalf("want active line 0.17 with nodes running 0.18 or later, got line=%q running=%q: %v", line, running, err)
		}
	})

	must("before/02_the_free_tier_has_the_allowance_and_no_hbd", func(t *testing.T) {
		avail, ceiling := rcNow(t)
		if ceiling <= 0 || avail <= 0 {
			t.Fatalf("%s reads %d of %d RC; it should hold the free allowance", payer, avail, ceiling)
		}
		t.Logf("%s: %d of %d RC available, records %v", payer, avail, ceiling, records(t))
	})

	must("before/03_it_spends_the_whole_allowance", func(t *testing.T) {
		drain(t, "before the line")
		// RC-GQL-2: an account with no balance record is still reported minus
		// what it has frozen. Before that fix this read was a flat 10,000.
		if avail, max := rcNow(t); avail >= max {
			t.Fatalf("available %d of a %d ceiling after spending the allowance: an account with no balance "+
				"record must still be reported minus what it has frozen (RC-GQL-2)", avail, max)
		}
	})

	must("before/04_every_retry_drags_the_anchor_to_the_head_and_the_return_is_lost", func(t *testing.T) {
		base := records(t)
		startHeight := nodeBlock(t, d, ctx, rcpRefNode)
		for i := 0; i < rctRetries; i++ {
			retry(t, fmt.Sprintf("retry before-%d", i+1))
		}
		head := nodeBlock(t, d, ctx, rcpRefNode)
		spanned := head - startHeight
		after := records(t)
		amount, _ := rcNow(t)
		unbroken := int64(spanned) / rctReturnBlocksPerRc

		if len(after) <= len(base) {
			t.Fatalf("%d refused calls over %d blocks wrote no RC record (%v); below the line each one is "+
				"charged %d RC the account does not have", rctRetries, spanned, after, rctCallMinRc)
		}
		if behind := head - anchor(t); behind > 40 {
			t.Fatalf("the newest RC record sits %d blocks behind the head; below the line every retry "+
				"re-anchors it, so it should stay with the head (records %v)", behind, after)
		}
		if unbroken < 4 {
			t.Fatalf("the retries only spanned %d blocks, worth %d RC of return; the test needs a span that "+
				"would plainly have returned something", spanned, unbroken)
		}
		if amount*2 > unbroken {
			t.Fatalf("available RC is %d after %d retries over %d blocks; an unbroken return would be about %d, "+
				"and below the line the re-anchor should have thrown nearly all of it away",
				amount, rctRetries, spanned, unbroken)
		}
		t.Logf("%d retries over %d blocks: %d records (was %d), newest %s, %d RC available of the %d an "+
			"unbroken return would have given", rctRetries, spanned, len(after), len(base), after[len(after)-1], amount, unbroken)
	})

	// ------------------------------------------------------------------
	// The on-chain proposal raises the floor to 0.18.
	// ------------------------------------------------------------------
	must("switch/05_the_proposal_raises_the_floor_to_0.18", func(t *testing.T) {
		epoch, height := rcpRaiseFloor(t, d, ctx, nodes, 18, 40*time.Minute)
		t.Logf("0.18.0 carried by the election of epoch %d at block %d", epoch, height)
		if err := pollAllNodes(nodes, 10*time.Minute, func(n int) bool {
			line, _, _, e := d.ConsensusInfo(ctx, n)
			return e == nil && line == "0.18"
		}); err != nil {
			t.Fatalf("not every node adopted 0.18: %v", err)
		}
		// Two slots of room, so nothing below is measured against a slot that
		// was still being charged under the old rule.
		if err := d.WaitForBlockProcessing(ctx, rcpRefNode, nodeBlock(t, d, ctx, rcpRefNode)+20, 5*time.Minute); err != nil {
			t.Fatalf("waiting out the activation slot: %v", err)
		}
	})

	// ------------------------------------------------------------------
	// After: the chain runs consensus 0.18.
	// ------------------------------------------------------------------
	must("after/06_an_account_that_can_pay_is_still_charged_in_full", func(t *testing.T) {
		// The rule is "all of the charge or none of it", so the half that must
		// NOT change needs proving too: a payer with RC is charged exactly what
		// it was before. A witness holds 990 HBD on the ledger, so it can pay —
		// unlike the free-tier account, which cannot until its return has run
		// for about 1,440 blocks (after/08).
		//
		// Without this case a fix that simply stopped charging anything would
		// pass every other case here.
		w3 := d.witnessAccount(3)
		o := rcpMeasure(t, d, ctx, w3, cid, "noop", "x", rctRetryLimit, nil)
		if o.Status != "CONFIRMED" || o.Used != rctCallMinRc {
			t.Fatalf("a funded account's call: status %s, charged %d RC, want CONFIRMED and exactly the %d "+
				"minimum — 0.18.0 must not stop charging a payer that can pay", o.Status, o.Used, rctCallMinRc)
		}
		t.Logf("hive:%s (funded) was charged %d RC for the same call the free tier is refused", w3, o.Used)
	})

	must("after/06b_the_free_tier_still_cannot_pay_for_a_call", func(t *testing.T) {
		// The premise of the next case, re-established however many blocks the
		// switch took: if the return has already carried the account past the
		// minimum, spend it back down.
		if avail, _ := rcNow(t); avail >= rctCallMinRc {
			t.Logf("the switch took long enough for the account to return to %d RC; draining it again", avail)
			drain(t, "after the line")
			return
		}
		avail, _ := rcNow(t)
		t.Logf("%s: %d RC available, under the %d a call costs", payer, avail, rctCallMinRc)
	})

	must("after/07_a_retry_with_no_rc_is_not_charged_and_leaves_the_record_alone", func(t *testing.T) {
		base := records(t)
		baseAnchor := anchor(t)
		baseAmount, _ := rcNow(t)
		startHeight := nodeBlock(t, d, ctx, rcpRefNode)
		var mid int64
		for i := 0; i < rctRetries; i++ {
			retry(t, fmt.Sprintf("retry after-%d", i+1))
			if i == rctRetries/2-1 {
				mid, _ = rcNow(t)
			}
		}
		head := nodeBlock(t, d, ctx, rcpRefNode)
		spanned := head - startHeight
		after := records(t)
		amount, _ := rcNow(t)
		gain := amount - baseAmount
		unbroken := int64(spanned) / rctReturnBlocksPerRc

		if len(after) != len(base) || after[len(after)-1] != base[len(base)-1] {
			t.Fatalf("%d refused calls wrote the account's RC record: %v -> %v. A payer that cannot cover the "+
				"charge is not charged, so the slot must leave its record alone", rctRetries, base, after)
		}
		if a := anchor(t); a != baseAnchor {
			t.Fatalf("the return anchor moved %d -> %d while the account was being refused", baseAnchor, a)
		}
		if gain*2 < unbroken {
			t.Fatalf("available RC went %d -> %d (gained %d) over %d blocks of retrying; an unbroken return is "+
				"about %d RC and the retries must not eat it", baseAmount, amount, gain, spanned, unbroken)
		}
		if mid <= baseAmount || amount <= mid {
			t.Fatalf("available RC did not climb steadily: %d -> %d -> %d over %d blocks", baseAmount, mid, amount, spanned)
		}
		t.Logf("%d retries over %d blocks: records unchanged (%v), available %d -> %d -> %d RC (an unbroken "+
			"return over the span is about %d)", rctRetries, spanned, after, baseAmount, mid, amount, unbroken)
	})

	t.Run("after/08_the_free_tier_itself_is_charged_again_once_it_can_pay", func(t *testing.T) {
		// The return is (frozen / RC_RETURN_PERIOD) a block — 0.069 RC a block
		// off the free allowance — so the 100 RC a call costs takes about 1,440
		// blocks. RC_RETURN_PERIOD is a source constant, not a consensus
		// parameter a devnet can shorten, so those blocks have to pass.
		//
		// They cost 1,440 x 3s = 72 minutes of the TEST CLOCK (devclock.go): one
		// block is three seconds of it however fast the harness really mines
		// them, and the devnet's whole budget is spent on the same clock. The
		// default budget cap is 75 minutes, which the earlier phases have
		// already eaten into — so without a raised cap this wait would expire
		// the devnet's context mid-case and take the nodes down with it, which
		// is what it did before this guard. Skip instead.
		//
		// after/06 proves the charge still lands with a funded payer, so a skip
		// here leaves nothing unproven about the rule — only about this account
		// walking the whole way back.
		const returnBlocks = rctCallMinRc * rctReturnBlocksPerRc // 1,440
		need := returnBlocks * 3 * time.Second
		left := time.Duration(0)
		if dl, ok := ctx.Deadline(); ok {
			left = devnetUntil(dl)
		}
		if left < need+10*time.Minute {
			t.Skipf("the %d RC a call costs needs %s of test clock to return (%d blocks) and the devnet budget "+
				"has %s left: run with DEVNET_BUDGET_CAP=150m and a matching `go test -timeout` to cover this "+
				"case. What the rule does is proven either way by after/06 and by the unit tests",
				rctCallMinRc, need, returnBlocks, left.Round(time.Minute))
		}
		// Advance the chain the harness's way rather than sleeping: under fast
		// blocks it mines toward the target as quickly as the nodes keep up.
		head := nodeBlock(t, d, ctx, rcpRefNode)
		var amount int64
		for {
			if amount, _ = rcNow(t); amount >= rctCallMinRc {
				break
			}
			cur := nodeBlock(t, d, ctx, rcpRefNode)
			if cur-head > returnBlocks+120 {
				t.Fatalf("after %d blocks of return the account has %d RC, want the %d a call costs; the "+
					"return should give one RC every %d blocks", cur-head, amount, rctCallMinRc, rctReturnBlocksPerRc)
			}
			if err := d.WaitForBlockProcessing(ctx, rcpRefNode, cur+120, 20*time.Minute); err != nil {
				t.Fatalf("advancing the chain for the return: %v", err)
			}
		}
		t.Logf("the account returned to %d RC over %d blocks", amount, nodeBlock(t, d, ctx, rcpRefNode)-head)
		prev, havePrev, err := rcpLatest(ctx, d, rcpRefNode, payer)
		if err != nil {
			t.Fatalf("reading %s RC record: %v", payer, err)
		}
		before := records(t)
		status := send(t, "the first call it can pay for again")
		charged := rcpWaitCharge(t, d, ctx, payer, prev, havePrev)
		if charged != rctCallMinRc {
			t.Fatalf("with %d RC available the call was charged %d (status %s), want exactly the %d RC minimum: "+
				"the rule is all of the charge or none of it, not that a call with no RC behind it is free "+
				"(records %v -> %v)", amount, charged, status, rctCallMinRc, before, records(t))
		}
		t.Logf("at %d RC available the call was charged %d RC again (status %s); records %v",
			amount, charged, status, records(t))
	})

	t.Run("after/09_every_node_holds_the_same_rc_records", func(t *testing.T) {
		rcpNodesAgreeOnRc(t, d, ctx, allNodes, []string{payer})
	})
}
