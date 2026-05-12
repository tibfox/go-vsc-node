package mapper

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"vsc-node/cmd/mapping-bot/chain"
	"vsc-node/cmd/mapping-bot/database"
)

// instantSendDedup is an in-memory set of txids we've already submitted via
// mapInstantSend. The on-chain contract is the source of truth for
// idempotency (its ISLockedClaimed marker aborts duplicate calls), but we
// also dedup locally to avoid pointless network round-trips. Bounded to
// avoid leaking under sustained traffic.
type instantSendDedup struct {
	mu    sync.Mutex
	seen  map[string]time.Time
	order []string
}

const instantSendDedupMax = 100_000

func newInstantSendDedup() *instantSendDedup {
	return &instantSendDedup{seen: make(map[string]time.Time)}
}

func (d *instantSendDedup) tryClaim(txid string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.seen[txid]; exists {
		return false
	}
	d.seen[txid] = time.Now()
	d.order = append(d.order, txid)
	if len(d.order) > instantSendDedupMax {
		evict := d.order[0]
		d.order = d.order[1:]
		delete(d.seen, evict)
	}
	return true
}

// isMapInstantSendParams is the wire shape the contract's mapInstantSend
// action expects. Kept package-local: this is the only place we marshal it.
type isMapInstantSendParams struct {
	RawTxHex     string   `json:"raw_tx_hex"`
	Instructions []string `json:"instructions"`
}

// ConsumeDeposits subscribes to the chain client's real-time deposit feed (if
// any) and forwards each InstantSend-locked event to HandleInstantSendDeposit.
// Plain (non-IS-locked) events are ignored — the existing block-driven flow
// picks those up once they confirm.
//
// Runs until the context is cancelled or the deposit channel is closed.
// Designed to be launched once in a goroutine from the bot's main loop.
//
// Returns immediately with a logged note when the underlying client does not
// implement DepositNotifier (no error — non-Dash chains take this branch).
func (b *Bot) ConsumeDeposits(ctx context.Context) {
	notifier, ok := b.Chain.Client.(chain.DepositNotifier)
	if !ok {
		b.L.Info("chain client has no real-time deposit feed; skipping IS consumer",
			"chain", b.Chain.Name)
		return
	}
	dedup := newInstantSendDedup()
	events := notifier.Deposits()
	b.L.Info("starting InstantSend deposit consumer", "chain", b.Chain.Name)
	for {
		select {
		case <-ctx.Done():
			b.L.Info("InstantSend consumer stopping", "reason", ctx.Err())
			return
		case ev, ok := <-events:
			if !ok {
				b.L.Info("InstantSend deposit channel closed")
				return
			}
			if !ev.InstantLocked {
				// Non-IS deposits go through the normal block-driven flow.
				continue
			}
			if !dedup.tryClaim(ev.TxID) {
				continue
			}
			go b.HandleInstantSendDeposit(ev)
		}
	}
}

// HandleInstantSendDeposit submits a single IS-locked deposit to the contract
// via the mapInstantSend action. Looks up the deposit address's stored
// instruction from the local address registry; logs and returns if the
// address is unknown.
//
// Errors are logged but not returned — the contract's ISLockedClaimed marker
// makes the call idempotent, so re-submits are at worst noisy and the bot
// can recover by simply seeing the same event again on a future ZMQ
// replay (won't happen in practice) or by the normal block-driven map flow
// once the tx confirms.
func (b *Bot) HandleInstantSendDeposit(ev chain.DepositEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	instruction, err := b.addrDB().GetInstruction(ctx, ev.Address)
	if err != nil {
		if errors.Is(err, database.ErrAddrNotFound) {
			// Address isn't (or no longer is) in our registry. ZMQ filtered
			// it but we deregistered before processing — benign race.
			b.L.Debug("IS deposit for unknown address, skipping",
				"address", ev.Address, "txid", ev.TxID)
			return
		}
		b.L.Error("IS deposit: failed to look up instruction",
			"address", ev.Address, "txid", ev.TxID, "err", err)
		return
	}

	payload, err := json.Marshal(isMapInstantSendParams{
		RawTxHex:     ev.RawTxHex,
		Instructions: []string{instruction},
	})
	if err != nil {
		b.L.Error("IS deposit: marshal payload failed",
			"txid", ev.TxID, "err", err)
		return
	}

	b.L.Info("submitting IS-locked deposit via mapInstantSend",
		"txid", ev.TxID, "address", ev.Address, "instruction", instruction)
	if _, err := b.callWithRetry(ctx, json.RawMessage(payload), "mapInstantSend", broadcastRetryAttempts); err != nil {
		b.L.Error("mapInstantSend call failed", "txid", ev.TxID, "err", err)
	}
}

