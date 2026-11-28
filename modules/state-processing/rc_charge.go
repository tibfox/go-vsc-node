package state_engine

import (
	"vsc-node/modules/common/consensusversion"
	rcSystem "vsc-node/modules/rc-system"
	transactionpool "vsc-node/modules/transaction-pool"
)

// RcChargeIfAffordable is what an account is charged for a transaction that
// wants `want` RC: all of it, or none of it. `available` is its RC at the
// transaction's height (its HBD balance plus the free allowance, less what is
// still frozen) and `already` is what this slot has charged it so far.
//
// All or nothing, not the part it can cover, because a part is worse than
// either: it takes the RC the account has just earned back without buying it
// anything, and — through the re-stamp UpdateRcMap does for any charge — it
// re-anchors the return schedule for the account's whole frozen total. An
// account retrying with nothing left would be held at nothing by its own
// trickle. See consensusversion.UnaffordableRcNotChargedActive.
//
// Nothing is lost by not charging it: the clamp in UpdateRcMap already caps the
// stored total at the account's ceiling, so RC charged beyond what the account
// has was never collected — it only moved the anchor.
func RcChargeIfAffordable(available, already, want int64) int64 {
	if want <= 0 {
		return 0
	}
	if available-already < want {
		return 0
	}
	return want
}

// ChargeRc charges `payer` want RC for a transaction at blockHeight into the
// slot's RC map, which UpdateRcMap folds into the payer's RC record when the
// slot closes. It is the one place a transaction's RC reaches se.RcMap.
//
// From 0.18.0 (consensusversion.UnaffordableRcNotChargedActive) a payer that
// cannot cover the charge is not charged at all, and an uncharged payer is left
// out of the RC map entirely — so the slot does not write its RC record, and an
// account with nothing left keeps the return it has already earned instead of
// having it re-anchored by a transaction it paid nothing for. Below the line
// want is added verbatim, as it always was, so settled history re-executes to
// the RC it froze.
//
// `rcs` reads the payer's available RC through the slot's ledger session, so it
// sees the balance changes earlier transactions in the slot made. It must be a
// session that has not consumed anything itself (one per transaction, as
// ExecuteBatch builds them): what the slot has charged already is se.RcMap,
// which is passed to RcChargeIfAffordable separately.
func (se *StateEngine) ChargeRc(rcs rcSystem.RcSession, payer string, blockHeight uint64, want int64) {
	already := se.RcMap[payer]
	if !consensusversion.UnaffordableRcNotChargedActive(se.ActiveConsensusVersion(blockHeight)) {
		se.RcMap[payer] = already + want
		return
	}
	if payer == "" || rcs == nil {
		return
	}
	_, available, _ := rcs.CanConsume(payer, blockHeight, 0)
	charge := RcChargeIfAffordable(available, already, want)
	if charge == 0 {
		return
	}
	se.RcMap[payer] = already + charge
}

// RcChargedInSlot implements common_types.StateEngine: the RC this slot has
// already charged `account`. Zero for an account the slot has not charged.
func (se *StateEngine) RcChargedInSlot(account string) int64 {
	return se.RcMap[account]
}

// RcAffordable reports whether `payer` can cover a `want` RC charge at
// blockHeight, counting what this slot has already charged it. It is the gate
// form of RcChargeIfAffordable, deliberately the same predicate: an operation
// is allowed to run only when the charge that follows it will actually land
// (RC-SLOT-GATE-1 / RC-UNGATED-OPS-1, 0.18.0).
//
// It fails OPEN when there is nothing to price the payer with — no payer, no RC
// session — because the gate must not start refusing operations it cannot
// price. ChargeRc charges nothing in the same case, so such an operation runs
// exactly as it did below the line.
func (se *StateEngine) RcAffordable(rcs rcSystem.RcSession, payer string, blockHeight uint64, want int64) bool {
	if want <= 0 {
		return true
	}
	if payer == "" || rcs == nil {
		return true
	}
	_, available, _ := rcs.CanConsume(payer, blockHeight, 0)
	return RcChargeIfAffordable(available, se.RcMap[payer], want) == want
}

// RcRemainingInSlot is the RC `payer` has left to spend at blockHeight: what it
// holds, less what this slot has already charged it. Never negative — a payer
// whose slot charges have overrun what it holds has nothing left, not a debt.
// Reported in a refusal so the message names a number the payer can check.
func (se *StateEngine) RcRemainingInSlot(rcs rcSystem.RcSession, payer string, blockHeight uint64) int64 {
	if payer == "" || rcs == nil {
		return 0
	}
	_, available, _ := rcs.CanConsume(payer, blockHeight, 0)
	if left := available - se.RcMap[payer]; left > 0 {
		return left
	}
	return 0
}

// OpRcCost is what an operation other than a contract call charges its payer:
// the RcUsed its ExecuteTx returns once it has reached the ledger, which is the
// same figure whether the ledger accepted it or refused it (both return the
// success-path cost; the cheaper 50 is for a validation failure before that).
// The gate in ExecuteBatch refuses an operation whose payer cannot cover this,
// so every operation that runs can pay for what it does — pricing the gate at
// the failure cost instead would sell a payer the successes it cannot afford.
//
// transfer, withdraw, stake_hbd and unstake_hbd mirror transactionpool's
// constants, the figures the ingestion gate and the block producer size a
// declared rc_limit against. consensus_stake and consensus_unstake have no
// constant there — StaticMaxRcCost prices them at RcCostUnknown (50), which
// understates them; left alone here because that is mempool policy and this is
// consensus. An unrecognized type keeps the 50 floor an undecodable
// transaction is charged, so a type added later is gated rather than free.
func OpRcCost(opType string) int64 {
	switch opType {
	case "transfer":
		return int64(transactionpool.RcCostTransfer)
	case "withdraw":
		return int64(transactionpool.RcCostWithdraw)
	case "stake_hbd":
		return int64(transactionpool.RcCostStake)
	case "unstake_hbd":
		return int64(transactionpool.RcCostUnstake)
	case "consensus_stake", "consensus_unstake":
		return 100
	default:
		return int64(transactionpool.RcCostUnknown)
	}
}
