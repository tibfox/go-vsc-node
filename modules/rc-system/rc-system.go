package rc_system

import (
	"math"
	"strings"
	"vsc-node/modules/common/consensusversion"
	rcDb "vsc-node/modules/db/vsc/rcs"
	ledgerSystem "vsc-node/modules/ledger-system"

	"github.com/chebyrash/promise"
)

type RcSystem struct {
	RcDb         rcDb.RcDb
	LedgerSystem ledgerSystem.LedgerSystem
	// HiveFreeAmount is the network's free-RC allowance for Hive accounts,
	// resolved once from SystemConfig at construction. VR2-17: it must NEVER be
	// read from the params package global on a consensus path — that global was
	// mutated at process start per -network, so two nodes running different
	// binaries on the same network computed different gas budgets, different
	// block CIDs, and stalled the fleet.
	HiveFreeAmount int64
	// ConsensusAt resolves the chain-active consensus version at a block
	// height, for the rules here that are gated on one. The state engine wires
	// its own fail-stop election read in (state_engine.go New), so every reader
	// that shares the engine's RcSystem — the transaction pool and the block
	// producer do — decides identically.
	//
	// Nil means there is no chain to ask: every gated rule here is then inert,
	// which is what every reader did before 0.18.0. rc-system is built
	// standalone in unit tests and by read-only tools.
	ConsensusAt func(blockHeight uint64) consensusversion.Version
}

// FrozenCapped reports whether an account's frozen RC is capped at its ceiling
// before the return is applied (RC-CEILING-OVERHANG-1, consensus 0.18.0).
func (rcs *RcSystem) FrozenCapped(blockHeight uint64) bool {
	if rcs == nil || rcs.ConsensusAt == nil {
		return false
	}
	return consensusversion.FrozenRcCappedAtCeilingActive(rcs.ConsensusAt(blockHeight))
}

// EffectiveFrozenBal is the RC still frozen for an account: the amount its RC
// record stored, capped at what the account can owe (`ceiling`: its HBD balance
// plus the free allowance), and then decayed over the blocks since the record
// was written.
//
// The cap is RC-CEILING-OVERHANG-1 and is why this exists; `capped` is the
// 0.18.0 gate (consensusversion.FrozenRcCappedAtCeilingActive), and with it
// false this is exactly CalculateFrozenBal on the stored amount — what every
// reader did before. The cap is applied to the amount the return is computed
// FROM, not to the result: capping the result leaves the account owing the same
// total for the same number of blocks and changes nothing it can spend.
//
// It is a view of the record and never written back, so a balance that returns
// brings its RC obligation back with it.
func EffectiveFrozenBal(recordHeight uint64, recordAmount, ceiling int64, blockHeight uint64, capped bool) int64 {
	amount := recordAmount
	if capped {
		if ceiling < 0 {
			ceiling = 0
		}
		if amount > ceiling {
			amount = ceiling
		}
	}
	return CalculateFrozenBal(recordHeight, blockHeight, amount)
}

// ceiling is the most RC an account can owe at a height: its HBD balance, plus
// the network's free allowance if it is a Hive account.
func (rcs *RcSystem) ceiling(account string, blockHeight uint64) int64 {
	balAmt := rcs.LedgerSystem.GetBalance(account, blockHeight, "hbd")
	if strings.HasPrefix(account, "hive:") {
		balAmt = balAmt + rcs.HiveFreeAmount
	}
	return balAmt
}

// frozenOwed is GetFrozenAmt for a caller that has already computed the
// account's ceiling — which is every caller that is about to subtract the
// result from it. Below the 0.18.0 line the ceiling is ignored.
func (rcs *RcSystem) frozenOwed(account string, blockHeight uint64, ceiling int64) int64 {
	rcRecord, _ := rcs.RcDb.GetRecord(account, blockHeight)

	// review2 MEDIUM #107: route through the overflow/negative/underflow-safe
	// shared helper instead of an inline int64(diff*uint64(amount)/period).
	return EffectiveFrozenBal(rcRecord.BlockHeight, rcRecord.Amount, ceiling, blockHeight, rcs.FrozenCapped(blockHeight))
}

// Returns the amount of RCs that are frozen for the given account at the given block height
func (rcs *RcSystem) GetFrozenAmt(account string, blockHeight uint64) int64 {
	var ceiling int64
	if rcs.FrozenCapped(blockHeight) {
		// Only read the balance when the cap needs it, so the path below the
		// line costs exactly what it did before.
		ceiling = rcs.ceiling(account, blockHeight)
	}
	return rcs.frozenOwed(account, blockHeight, ceiling)
}

func (rcs *RcSystem) GetAvailableRCs(account string, blockHeight uint64) int64 {
	// Oracle DIDs bypass RC limits entirely.
	if strings.HasPrefix(account, "did:vsc:oracle:") {
		return math.MaxInt64
	}

	balAmt := rcs.ceiling(account, blockHeight)

	frozeAmt := rcs.frozenOwed(account, blockHeight, balAmt)

	if frozeAmt > balAmt {
		frozeAmt = balAmt
	}

	return balAmt - frozeAmt
}

func (rcs *RcSystem) NewSession(ledgerSession ledgerSystem.LedgerSession) RcSession {
	return &rcSession{
		ledgerSession: ledgerSession,
		rcSystem:      rcs,

		rcMap: make(map[string]int64),
	}
}

func (rc *RcSystem) Init() error {
	return nil
}

func (rc *RcSystem) Start() *promise.Promise[any] {
	return promise.New(func(resolve func(interface{}), reject func(error)) {
		resolve(nil)
	})
}

func (rc *RcSystem) Stop() error {
	return nil
}

// New builds an RcSystem. hiveFreeAmount is the network's free-RC allowance,
// resolved by the caller from SystemConfig.RcHiveFreeAmount() so every node on a
// network uses the identical value (VR2-17).
func New(rcDb rcDb.RcDb, ledgerSystem ledgerSystem.LedgerSystem, hiveFreeAmount int64) *RcSystem {
	return &RcSystem{
		RcDb:           rcDb,
		LedgerSystem:   ledgerSystem,
		HiveFreeAmount: hiveFreeAmount,
	}
}

type rcSession struct {
	ledgerSession ledgerSystem.LedgerSession
	rcSystem      *RcSystem

	rcMap map[string]int64
}

func (rss *rcSession) Consume(account string, blockHeight uint64, rcAmt int64) (bool, int64) {
	canConsume, _, _ := rss.CanConsume(account, blockHeight, rcAmt)

	if canConsume {
		rss.rcMap[account] = rss.rcMap[account] + rcAmt
		return true, rcAmt
	} else {
		return false, 0
	}
}

func (rss *rcSession) CanConsume(account string, blockHeight uint64, rcAmt int64) (bool, int64, int64) {
	// Oracle DIDs are consensus-validated system accounts (2/3 BLS threshold).
	// They have no HBD balance and should never be rate-limited by RCs.
	// This matches IngestTx which also skips RC checks for VscDID accounts.
	if strings.HasPrefix(account, "did:vsc:oracle:") {
		return true, math.MaxInt64, rcAmt
	}

	balAmt := rss.ceiling(account, blockHeight)

	frozeAmt := rss.rcSystem.frozenOwed(account, blockHeight, balAmt)

	totalAmt := balAmt - frozeAmt - rss.rcMap[account]
	if totalAmt < rcAmt {
		return false, 0, rcAmt
	} else {
		return true, totalAmt - rcAmt, rcAmt
	}
}

// HiveFreeAmount exposes the network's free-RC allowance through the RcSession
// interface so package-level helpers (FreeRcRemaining) can reach it without
// reading the params global (VR2-17).
func (rss *rcSession) HiveFreeAmount() int64 {
	return rss.rcSystem.HiveFreeAmount
}

// ceiling is the most RC an account can owe at a height, read through the
// session's ledger so it sees the balance changes earlier transactions of the
// slot have made. Mirrors RcSystem.ceiling.
func (rss *rcSession) ceiling(account string, blockHeight uint64) int64 {
	balAmt := rss.ledgerSession.GetBalance(account, blockHeight, "hbd")
	if strings.HasPrefix(account, "hive:") {
		//Give the user the network's free RC allowance by default
		//If user is Hive account
		balAmt = balAmt + rss.rcSystem.HiveFreeAmount
	}
	return balAmt
}

// GetFrozenAmt on a session includes the session's pending (uncommitted)
// consumption in rcMap, so callers see a view consistent with CanConsume.
func (rss *rcSession) GetFrozenAmt(account string, blockHeight uint64) int64 {
	var ceiling int64
	if rss.rcSystem.FrozenCapped(blockHeight) {
		ceiling = rss.ceiling(account, blockHeight)
	}
	return rss.rcSystem.frozenOwed(account, blockHeight, ceiling) + rss.rcMap[account]
}

// FreeRcRemaining returns the portion of RC_HIVE_FREE_AMOUNT still available
// to account at blockHeight (0 for non-Hive accounts, and for Hive accounts
// whose frozen RCs already consumed the free tier).
func FreeRcRemaining(rs RcSession, account string, blockHeight uint64) int64 {
	if !strings.HasPrefix(account, "hive:") {
		return 0
	}
	remaining := rs.HiveFreeAmount() - rs.GetFrozenAmt(account, blockHeight)
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (rss *rcSession) Revert() {
	rss.rcMap = make(map[string]int64)
}

func (rss *rcSession) Done() RcMapResult {
	return RcMapResult{
		RcMap: rss.rcMap,
	}
}
