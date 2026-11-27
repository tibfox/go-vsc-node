package state_engine

import (
	"vsc-node/modules/common/consensusversion"
	rcSystem "vsc-node/modules/rc-system"
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
