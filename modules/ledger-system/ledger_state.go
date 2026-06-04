package ledgerSystem

import (
	"fmt"
	"slices"
	"time"
	ledger_db "vsc-node/modules/db/vsc/ledger"
)

// blockingLedgerRead runs read() until it returns nil, with capped exponential
// backoff. Fail-stop primitive for the ledger-system spend-check path: a
// balance read that one node completes but another swallows lets the two nodes
// decide a tx outcome differently — a consensus fork — and the nil slice
// pointer GetLedgerRange returns on a Mongo error panics the node if
// dereferenced. Blocking until the DB recovers keeps every honest node either
// computing the identical balance or making no progress, never crashing and
// never forking. Mirrors state-processing.blockingRetry (state_engine.go:2124).
func blockingLedgerRead(what string, read func() error) {
	const (
		baseDelay = 100 * time.Millisecond
		maxDelay  = 30 * time.Second
	)
	delay := baseDelay
	for attempt := 1; ; attempt++ {
		if err := read(); err == nil {
			if attempt > 1 {
				log.Error("ledger DB read recovered; resuming", "op", what, "attempts", attempt)
			}
			return
		} else {
			log.Error("ledger DB read failed; blocking until DB recovers (fail-stop)",
				"op", what, "attempt", attempt, "retryIn", delay.String(), "err", err)
		}
		time.Sleep(delay)
		if delay < maxDelay {
			if delay *= 2; delay > maxDelay {
				delay = maxDelay
			}
		}
	}
}

// Used to represent the global ledger state in the execution environment
type LedgerState struct {
	//List of finalized operations such as transfers, withdrawals.
	//Expected to be identical when block is produced and used during block creation
	Oplog []OpLogEvent
	//Virtual ledger is a cache of all balance changes (virtual and non-virtual)
	//Includes deposits, transfers (in-n-out), withdrawals, and stake/unstake operations (future)
	VirtualLedger map[string][]LedgerUpdate

	//Live calculated gateway balances on the fly
	//Use last saved balance as the starting data
	GatewayBalances map[string]uint64

	//Block height of the operation to be processed at
	BlockHeight uint64

	//Potential database state access

	LedgerDb  ledger_db.Ledger
	ActionDb  ledger_db.BridgeActions
	BalanceDb ledger_db.Balances
}

func (state *LedgerState) Validate() {
	// if state.BlockHeight == 0 {
	// 	panic("invalid ledgerState instance: BlockHeight is 0")
	// }
	if state.LedgerDb == nil || state.ActionDb == nil || state.BalanceDb == nil {
		panic("invalid ledgerState instance: LedgerDb is nil")
	}
}

// func (le *LedgerState) AppendLedger(update LedgerUpdate) {
// 	key := update.Owner + "#" + update.Asset
// 	if le.GatewayBalances[key] == 0 {
// 		le.Ls.GetBalance(update.Owner, update.BlockHeight, update.Asset)
// 	}
// }

func (le *LedgerState) Export() struct {
	Oplog []OpLogEvent
} {
	oplogCP := make([]OpLogEvent, len(le.Oplog))
	copy(oplogCP, le.Oplog)

	return struct {
		Oplog []OpLogEvent
	}{
		Oplog: oplogCP,
	}
}

func (state *LedgerState) Flush() {
	state.VirtualLedger = make(map[string][]LedgerUpdate)
	state.Oplog = make([]OpLogEvent, 0)

	//qq: should this be cleared when flushing?
	state.GatewayBalances = make(map[string]uint64)
}

func (state *LedgerState) Compile(bh uint64) *CompiledResult {
	if len(state.Oplog) == 0 {
		return nil
	}
	oplog := make([]OpLogEvent, 0)
	// copy(oplog, le.Oplog)

	for _, v := range state.Oplog {
		//bh should be == slot height
		if v.BlockHeight <= bh {
			oplog = append(oplog, v)
		}
	}

	return &CompiledResult{
		OpLog: oplog,
	}
}

// Original ledger executor
func (state *LedgerState) SnapshotForAccount(account string, blockHeight uint64, asset string) int64 {
	bal := state.GetBalance(account, blockHeight, asset)

	//le.Ls.log.Debug("getBalance le.VirtualLedger["+account+"]", le.VirtualLedger[account], blockHeight)

	for _, v := range state.VirtualLedger[account] {
		//Must be ledger ops with height below or equal to the current block height
		//Current block height ledger ops are recently executed
		if v.Asset == asset {
			bal += v.Amount
		}
	}
	return bal
}

// isLedgerMetaRow reports whether a ledger record type is protocol meta state
// (safety-slash burn queue / finalize cursor / restitution-claim queue) rather
// than a spendable balance movement. These rows live on protocol-owned accounts
// and represent queue/cursor state, never spendable HIVE on the holder's own
// account, so they are excluded from every balance summation. Mirrors the skip
// list in StateEngine.UpdateBalances (state_engine.go:2038-2053).
func isLedgerMetaRow(t string) bool {
	switch t {
	case LedgerTypeSafetySlashHiveBurn,
		LedgerTypeSafetySlashHiveBurnPending,
		LedgerTypeSafetySlashHiveBurnPendingRelease,
		LedgerTypeSafetySlashHiveBurnPendingFinalized,
		LedgerTypeSafetySlashHiveBurnPendingCancelled,
		LedgerTypeSafetySlashBurnFinalizeCursor,
		LedgerTypeSafetyRestitutionClaim,
		LedgerTypeSafetyRestitutionClaimConsumed:
		return true
	default:
		return false
	}
}

// GetBalance is the authoritative spendable-balance read used by every
// spend-check in the ledger session (ledger_session.go:167/310/356/467 …). It
// MUST agree, for the same (account, blockHeight, asset), with the snapshot
// that StateEngine.UpdateBalances writes — otherwise the spend gate and the
// finalized snapshot diverge.
//
// review7 CRIT-1: this previously summed a positives-only OpType whitelist per
// asset (hbd→{unstake,deposit}, hive→{deposit,restitution}, hbd_savings→{stake}),
// which silently dropped every outgoing debit committed after the last snapshot
// (transfer-out, withdraw, stake-out, consensus_stake-out, unstake-out). The
// read over-reported, and the gate `(fromBal - exclusion) < amount` let the
// same funds be spent twice (gateway insolvency). UpdateBalances instead sums
// EVERY record past the snapshot (empty OpType filter, state_engine.go:2011)
// minus the safety-slash meta rows. GetBalance now does exactly the same, so the
// two can never drift again: start from the BalanceDb snapshot field for the
// asset, then add the net of every non-meta LedgerDb record past the snapshot.
func (ls *LedgerState) GetBalance(account string, blockHeight uint64, asset string) int64 {
	if !slices.Contains(assetTypes, asset) {
		return 0
	}

	var recordHeight uint64
	var balRecord ledger_db.BalanceRecord
	var ledgerResults *[]ledger_db.LedgerRecord

	// GV-H1 (review7): both DB reads fail-stop. The prior code discarded the
	// GetLedgerRange error and dereferenced the nil slice pointer it returns on
	// a Mongo fault, panicking the node mid-spend-check; and silently treating
	// the error as "no records" would compute a balance from a partial read and
	// fork this node from healthy peers. Block until both reads succeed so the
	// balance is either correct or never returned. Empty OpType filter: sum ALL
	// records for this asset past the snapshot, exactly like UpdateBalances —
	// never a per-asset whitelist that can drift.
	blockingLedgerRead(fmt.Sprintf("GetBalance(%s @%d %s)", account, blockHeight, asset), func() error {
		balRecordPtr, err := ls.BalanceDb.GetBalanceRecord(account, blockHeight)
		if err != nil {
			return err
		}
		if balRecordPtr == nil {
			recordHeight = 0
			balRecord = ledger_db.BalanceRecord{}
		} else {
			balRecord = *balRecordPtr
			recordHeight = balRecord.BlockHeight + 1
		}

		results, err := ls.LedgerDb.GetLedgerRange(
			account,
			recordHeight,
			blockHeight,
			asset,
			ledger_db.LedgerOptions{},
		)
		if err != nil {
			return err
		}
		if results == nil {
			return fmt.Errorf("GetLedgerRange returned a nil result without an error")
		}
		ledgerResults = results
		return nil
	})

	balAdjust := int64(0)
	for _, v := range *ledgerResults {
		if isLedgerMetaRow(v.Type) {
			continue
		}
		balAdjust += v.Amount
	}

	switch asset {
	case "hbd":
		return balRecord.HBD + balAdjust
	case "hive":
		return balRecord.Hive + balAdjust
	case "hbd_savings":
		return balRecord.HBD_SAVINGS + balAdjust
	case "hive_consensus":
		return balRecord.HIVE_CONSENSUS + balAdjust
	default:
		return 0
	}
}
