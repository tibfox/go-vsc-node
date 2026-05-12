package chain

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

// DashdRPCClient implements BlockchainClient against a Dash Core JSON-RPC node.
//
// Designed for a *pruned* node — does NOT rely on `-txindex` or `-addressindex`
// (both are incompatible with pruning). Instead:
//   - GetAddressTxs uses `scantxoutset` to enumerate unspent outputs at the
//     address. This means only deposits that have NOT yet been swept appear in
//     the result — which matches the bot's "find unprocessed deposits" use case.
//   - GetTxDetails reads `getrawtransaction <txid> 1`. Works for txs in mempool
//     and inside the pruning window (the most recent ~5 GB of blocks given our
//     `-prune=5000` setting). Txs older than the prune horizon return an error.
//
// InstantSend awareness: GetTxDetails surfaces the `instantlock` flag via the
// IsInstantLocked field of TxConfirmationDetails. Callers that want 0-conf
// finality can treat (instantlock == true) as confirmed even when no block
// height is set yet — though note the on-chain contract still requires SPV
// data (block height + hash + index), so for now we only mark a tx Confirmed
// after it actually lands in a block. Real-time IS notifications belong in a
// separate ZMQ subscriber (see future work).
type DashdRPCClient struct {
	rpcURL string
	user   string
	pass   string
	client *http.Client

	// txHeightCache maps txid → confirmed block height. Populated whenever we
	// see a txid alongside its height (e.g., via scantxoutset). Used by
	// getRawTxVerbose to supply the `blockhash` hint that pruned dashd needs
	// (without -txindex it can't resolve a txid back to a block on its own).
	// Bounded to maxTxHeightCacheEntries via FIFO eviction — no LRU needed
	// because recency doesn't strongly correlate with future lookups.
	txHeightMu    sync.RWMutex
	txHeightCache map[string]uint64
	txHeightOrder []string // FIFO eviction queue paired with the map

	// ZMQ-fed state. Populated by the goroutine started in AttachZMQ. All
	// nil/empty if ZMQ is not wired up (graceful degradation to poll-only
	// mode).
	zmq    *DashdZMQSubscriber
	params *chaincfg.Params // needed to decode addresses from raw tx scripts

	watchedMu sync.RWMutex
	watched   map[string]struct{}

	// mempoolEntries indexes mempool deposits by their destination address
	// — entries are pruned when the underlying tx appears in a block
	// (HashBlock event triggers a scantxoutset reconciliation).
	mempoolMu      sync.RWMutex
	mempoolEntries map[string]map[string]TxHistoryEntry // addr → txid → entry

	isLockedMu    sync.RWMutex
	isLocked      map[string]struct{} // set of txids known IS-locked
	isLockedOrder []string            // FIFO eviction queue
}

// Bounds on long-lived caches. Picked so worst-case memory stays well under
// 50 MB: 100k entries × ~80 bytes (key + map overhead) ≈ 8 MB per cache.
const (
	maxTxHeightCacheEntries = 100_000
	maxIsLockedEntries      = 100_000
)

// NewDashdRPCClient constructs a client. `rpcURL` should be a full URL
// including scheme + host + port, e.g. "http://vsc-dashd-testnet:19998".
func NewDashdRPCClient(httpClient *http.Client, rpcURL, user, pass string) *DashdRPCClient {
	return &DashdRPCClient{
		rpcURL:         rpcURL,
		user:           user,
		pass:           pass,
		client:         httpClient,
		txHeightCache:  make(map[string]uint64),
		watched:        make(map[string]struct{}),
		mempoolEntries: make(map[string]map[string]TxHistoryEntry),
		isLocked:       make(map[string]struct{}),
	}
}

// AttachZMQ wires a running DashdZMQSubscriber to this client. After this is
// called the client will:
//   - filter `rawtx` events by watched addresses (see WatchAddress) and
//     surface mempool deposits via GetAddressTxs;
//   - record txids from `rawtxlock` events and report them via
//     TxConfirmationDetails.IsInstantLocked;
//   - flush mempool entries on `hashblock` events (the next scantxoutset
//     poll picks up the confirmed UTXO).
//
// `params` is required for decoding output scripts to addresses; pass the
// same *chaincfg.Params the bot uses to derive deposit addresses (e.g.,
// dashTestNetParams).
//
// Safe to call once. Calling without an attached ZMQ subscriber leaves the
// client in poll-only mode (still functional, just no mempool/IS-lock
// awareness).
func (c *DashdRPCClient) AttachZMQ(zmq *DashdZMQSubscriber, params *chaincfg.Params) {
	if zmq == nil {
		return
	}
	c.zmq = zmq
	c.params = params
	go c.consumeZMQ()
}

// WatchAddress registers an address whose mempool deposits should be tracked
// via the ZMQ feed. Has no effect until AttachZMQ has been called. Idempotent.
func (c *DashdRPCClient) WatchAddress(addr string) {
	c.watchedMu.Lock()
	defer c.watchedMu.Unlock()
	c.watched[addr] = struct{}{}
}

// UnwatchAddress stops tracking mempool deposits for an address.
func (c *DashdRPCClient) UnwatchAddress(addr string) {
	c.watchedMu.Lock()
	delete(c.watched, addr)
	c.watchedMu.Unlock()
	c.mempoolMu.Lock()
	delete(c.mempoolEntries, addr)
	c.mempoolMu.Unlock()
}

func (c *DashdRPCClient) isWatched(addr string) bool {
	c.watchedMu.RLock()
	_, ok := c.watched[addr]
	c.watchedMu.RUnlock()
	return ok
}

func (c *DashdRPCClient) markIsLocked(txid string) {
	c.isLockedMu.Lock()
	defer c.isLockedMu.Unlock()
	if _, exists := c.isLocked[txid]; exists {
		return
	}
	c.isLocked[txid] = struct{}{}
	c.isLockedOrder = append(c.isLockedOrder, txid)
	if len(c.isLockedOrder) > maxIsLockedEntries {
		evict := c.isLockedOrder[0]
		c.isLockedOrder = c.isLockedOrder[1:]
		delete(c.isLocked, evict)
	}
}

func (c *DashdRPCClient) isInstantLocked(txid string) bool {
	c.isLockedMu.RLock()
	_, ok := c.isLocked[txid]
	c.isLockedMu.RUnlock()
	return ok
}

// addMempoolEntry stores a pending deposit (tx in mempool targeting a watched
// address) for later retrieval via GetAddressTxs.
func (c *DashdRPCClient) addMempoolEntry(addr string, entry TxHistoryEntry) {
	c.mempoolMu.Lock()
	defer c.mempoolMu.Unlock()
	bucket, ok := c.mempoolEntries[addr]
	if !ok {
		bucket = make(map[string]TxHistoryEntry)
		c.mempoolEntries[addr] = bucket
	}
	if existing, found := bucket[entry.TxID]; found {
		// Merge IS-lock status if a newer event upgrades it.
		if entry.Confirmed {
			existing.Confirmed = true
		}
		bucket[entry.TxID] = existing
		return
	}
	bucket[entry.TxID] = entry
}

// mempoolFor returns a snapshot of the mempool entries for an address.
func (c *DashdRPCClient) mempoolFor(addr string) []TxHistoryEntry {
	c.mempoolMu.RLock()
	defer c.mempoolMu.RUnlock()
	bucket := c.mempoolEntries[addr]
	if len(bucket) == 0 {
		return nil
	}
	out := make([]TxHistoryEntry, 0, len(bucket))
	for _, e := range bucket {
		out = append(out, e)
	}
	return out
}

// reconcileOnBlock evicts mempool entries for txs that have just landed in a
// block — the next scantxoutset poll will surface them as confirmed UTXOs.
// We don't have the txids inline (HashBlock delivers only the block hash);
// the simplest correct strategy is to drop entries older than ~1 block worth
// of time. For now we just truncate per-address buckets that have grown
// large enough to indicate stuck txs (over 1024 mempool entries → reset).
// A future improvement is to fetch the block and remove only the txs it
// actually contains.
func (c *DashdRPCClient) reconcileOnBlock() {
	c.mempoolMu.Lock()
	defer c.mempoolMu.Unlock()
	for addr, bucket := range c.mempoolEntries {
		if len(bucket) > 1024 {
			delete(c.mempoolEntries, addr)
		}
	}
}

// consumeZMQ runs until the ZMQ subscriber's context is cancelled.
func (c *DashdRPCClient) consumeZMQ() {
	for {
		select {
		case ev, ok := <-c.zmq.RawTx:
			if !ok {
				return
			}
			c.handleRawTx(ev, false)
		case ev, ok := <-c.zmq.RawTxLock:
			if !ok {
				return
			}
			c.handleRawTx(ev, true)
		case _, ok := <-c.zmq.HashBlock:
			if !ok {
				return
			}
			c.reconcileOnBlock()
		case _, ok := <-c.zmq.RawChainLock:
			if !ok {
				return
			}
			// ChainLock fires when a block becomes finalized; nothing for
			// us to do beyond what HashBlock already did.
		case <-c.zmq.ctx.Done():
			return
		}
	}
}

// handleRawTx decodes a rawtx or rawtxlock event. For rawtxlock it
// unconditionally records the txid as IS-locked; for both it scans outputs
// for hits against the watched-addresses set and adds matches to the
// mempool cache.
func (c *DashdRPCClient) handleRawTx(ev DashRawTxEvent, isLock bool) {
	if len(ev.RawTx) == 0 {
		return
	}
	var tx wire.MsgTx
	if err := tx.Deserialize(bytes.NewReader(ev.RawTx)); err != nil {
		slog.Warn("dashd zmq: failed to deserialize rawtx", "err", err, "topic_isLock", isLock)
		return
	}
	hash := tx.TxHash()
	txid := chainHashToTxID(&hash)

	if isLock {
		c.markIsLocked(txid)
	}

	// Output parsing requires chain params — if AttachZMQ wasn't called with
	// them, skip the watched-address filter (we still record IS-locks).
	if c.params == nil {
		return
	}

	for i, out := range tx.TxOut {
		addrs := extractBTCAddresses(out.PkScript, c.params)
		for _, addr := range addrs {
			if !c.isWatched(addr) {
				continue
			}
			entry := TxHistoryEntry{
				TxID:      txid,
				Confirmed: false, // mempool by definition
				Outputs: []TxOutput{
					{
						Address: addr,
						Value:   out.Value,
						Index:   uint32(i),
					},
				},
			}
			c.addMempoolEntry(addr, entry)
		}
	}
}

func chainHashToTxID(h *chainhash.Hash) string {
	// chainhash.Hash.String() returns the reversed-hex form used by
	// Bitcoin/Dash JSON-RPC.
	return h.String()
}

func (c *DashdRPCClient) cacheTxHeight(txid string, height uint64) {
	c.txHeightMu.Lock()
	defer c.txHeightMu.Unlock()
	if _, exists := c.txHeightCache[txid]; !exists {
		// Only enqueue + evict on new insertions.
		c.txHeightOrder = append(c.txHeightOrder, txid)
		if len(c.txHeightOrder) > maxTxHeightCacheEntries {
			evict := c.txHeightOrder[0]
			c.txHeightOrder = c.txHeightOrder[1:]
			delete(c.txHeightCache, evict)
		}
	}
	c.txHeightCache[txid] = height
}

func (c *DashdRPCClient) lookupTxHeight(txid string) (uint64, bool) {
	c.txHeightMu.RLock()
	defer c.txHeightMu.RUnlock()
	h, ok := c.txHeightCache[txid]
	return h, ok
}

// ---- JSON-RPC plumbing -----------------------------------------------------

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string {
	return fmt.Sprintf("dashd rpc error %d: %s", e.Code, e.Message)
}

// Bitcoin/Dash Core RPC error codes we care about.
const (
	rpcInvalidParameter   = -8  // "Block height out of range", "txid out of range", etc.
	rpcInvalidAddrOrKey   = -5  // "No such mempool or blockchain transaction"
)

func (c *DashdRPCClient) call(method string, params ...any) (json.RawMessage, error) {
	if params == nil {
		params = []any{}
	}
	body, err := json.Marshal(rpcRequest{
		JSONRPC: "1.0",
		ID:      "vsc-mapping-bot",
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, c.rpcURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.user, c.pass)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dashd rpc %s: %w", method, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("dashd rpc %s: %w", method, err)
	}

	// Some failures (e.g. wrong credentials) return non-JSON HTML. Catch those.
	if !strings.HasPrefix(strings.TrimSpace(string(respBody)), "{") {
		return nil, fmt.Errorf("dashd rpc %s: non-JSON response (status %d): %s",
			method, resp.StatusCode, truncate(string(respBody), 200))
	}

	var r rpcResponse
	if err := json.Unmarshal(respBody, &r); err != nil {
		return nil, fmt.Errorf("dashd rpc %s: decode: %w", method, err)
	}
	if r.Error != nil {
		return nil, r.Error
	}
	return r.Result, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func isRPCErrorCode(err error, code int) bool {
	var e *rpcError
	return errors.As(err, &e) && e.Code == code
}

// ---- BlockchainClient interface -------------------------------------------

func (c *DashdRPCClient) GetTipHeight() (uint64, error) {
	raw, err := c.call("getblockcount")
	if err != nil {
		return 0, err
	}
	var n uint64
	if err := json.Unmarshal(raw, &n); err != nil {
		return 0, fmt.Errorf("decode block count: %w", err)
	}
	return n, nil
}

func (c *DashdRPCClient) GetBlockHashAtHeight(height uint64) (string, int, error) {
	raw, err := c.call("getblockhash", height)
	if err != nil {
		if isRPCErrorCode(err, rpcInvalidParameter) {
			// Block height out of range — equivalent to a 404.
			return "", http.StatusNotFound, nil
		}
		return "", 0, err
	}
	var hash string
	if err := json.Unmarshal(raw, &hash); err != nil {
		return "", 0, fmt.Errorf("decode block hash: %w", err)
	}
	return hash, http.StatusOK, nil
}

func (c *DashdRPCClient) GetRawBlock(hash string) ([]byte, error) {
	// Verbosity 0 returns the serialized hex.
	raw, err := c.call("getblock", hash, 0)
	if err != nil {
		return nil, err
	}
	var hexStr string
	if err := json.Unmarshal(raw, &hexStr); err != nil {
		return nil, fmt.Errorf("decode raw block: %w", err)
	}
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, fmt.Errorf("decode block hex: %w", err)
	}
	return b, nil
}

// scantxoutset response shape (we only need a subset of fields).
type scanUnspent struct {
	TxID   string  `json:"txid"`
	VOut   uint32  `json:"vout"`
	Amount float64 `json:"amount"` // DASH (not duffs)
	Height uint64  `json:"height"`
}

type scanResult struct {
	Success  bool          `json:"success"`
	Unspents []scanUnspent `json:"unspents"`
}

// getrawtransaction verbose=1 response (subset).
type rawTxVerbose struct {
	TxID          string  `json:"txid"`
	Confirmations int64   `json:"confirmations"`
	BlockHash     string  `json:"blockhash"`
	BlockHeight   uint64  `json:"height"`
	InstantLock   bool    `json:"instantlock"`
	Vout          []rawTxVout `json:"vout"`
}

type rawTxVout struct {
	Value        float64 `json:"value"` // DASH
	N            uint32  `json:"n"`
	ScriptPubKey struct {
		Addresses []string `json:"addresses"`
		Address   string   `json:"address"` // newer dashd uses singular
		Type      string   `json:"type"`
	} `json:"scriptPubKey"`
}

func (c *DashdRPCClient) getRawTxVerbose(txid string) (*rawTxVerbose, error) {
	// First try without a block-hash hint. Works for mempool txs and (if
	// -txindex were enabled) any historical tx. On a pruned node without
	// txindex this fails for confirmed txs with code -5.
	raw, err := c.call("getrawtransaction", txid, 1)
	if err == nil {
		var tx rawTxVerbose
		if err := json.Unmarshal(raw, &tx); err != nil {
			return nil, fmt.Errorf("decode rawtx: %w", err)
		}
		return &tx, nil
	}
	if !isRPCErrorCode(err, rpcInvalidAddrOrKey) {
		return nil, err
	}

	// Pruned-node fallback: look up the cached height for this txid (populated
	// by GetAddressTxs from scantxoutset), resolve to a block hash, retry
	// with the hint. If we have no cached height the tx is invisible to us
	// — surface the original "not found" error and let the caller treat it
	// as unconfirmed.
	height, ok := c.lookupTxHeight(txid)
	if !ok || height == 0 {
		return nil, err
	}
	blockhash, _, hErr := c.GetBlockHashAtHeight(height)
	if hErr != nil || blockhash == "" {
		return nil, err
	}
	raw, err = c.call("getrawtransaction", txid, 1, blockhash)
	if err != nil {
		return nil, err
	}
	var tx rawTxVerbose
	if err := json.Unmarshal(raw, &tx); err != nil {
		return nil, fmt.Errorf("decode rawtx: %w", err)
	}
	return &tx, nil
}

func dashToDuffs(amount float64) int64 {
	// 1 DASH = 1e8 duffs. Round to avoid float drift.
	return int64(amount*1e8 + 0.5)
}

func outputAddress(vout rawTxVout) string {
	if vout.ScriptPubKey.Address != "" {
		return vout.ScriptPubKey.Address
	}
	if len(vout.ScriptPubKey.Addresses) > 0 {
		return vout.ScriptPubKey.Addresses[0]
	}
	return ""
}

// GetAddressTxs returns the *unspent* txs currently funding the address.
//
// On a pruned node we can't enumerate historical (already-spent) txs without
// an address index. For the mapping-bot's use case this is fine: a deposit
// that has already been processed gets swept, so its UTXO no longer exists
// at the watched address — we naturally only see unprocessed deposits.
func (c *DashdRPCClient) GetAddressTxs(address string) ([]TxHistoryEntry, error) {
	descriptors := []string{fmt.Sprintf("addr(%s)", address)}
	raw, err := c.call("scantxoutset", "start", descriptors)
	if err != nil {
		return nil, fmt.Errorf("scantxoutset %s: %w", address, err)
	}
	var res scanResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, fmt.Errorf("decode scantxoutset: %w", err)
	}
	if !res.Success {
		return nil, fmt.Errorf("scantxoutset %s: not successful", address)
	}

	// Group UTXOs by txid (a single tx can have multiple outputs to the address).
	// Cache height → so GetTxDetails can pass it as a blockhash hint later
	// (required on pruned dashd without -txindex).
	byTx := make(map[string][]TxOutput, len(res.Unspents))
	confirmed := make(map[string]bool, len(res.Unspents))
	for _, u := range res.Unspents {
		// height == 0 means mempool (unconfirmed). scantxoutset technically
		// scans only the confirmed UTXO set, so we don't normally see height=0
		// here — but be defensive.
		confirmed[u.TxID] = u.Height > 0
		if u.Height > 0 {
			c.cacheTxHeight(u.TxID, u.Height)
		}
		byTx[u.TxID] = append(byTx[u.TxID], TxOutput{
			Address: address,
			Value:   dashToDuffs(u.Amount),
			Index:   u.VOut,
		})
	}

	entries := make([]TxHistoryEntry, 0, len(byTx))
	seen := make(map[string]struct{}, len(byTx))
	for txid, outs := range byTx {
		entries = append(entries, TxHistoryEntry{
			TxID:      txid,
			Confirmed: confirmed[txid],
			Outputs:   outs,
		})
		seen[txid] = struct{}{}
	}

	// Union in mempool entries from the ZMQ feed (unconfirmed deposits that
	// scantxoutset can't see). De-dup by txid in case the same tx surfaced
	// from both paths during a race.
	for _, m := range c.mempoolFor(address) {
		if _, dup := seen[m.TxID]; dup {
			continue
		}
		entries = append(entries, m)
	}
	return entries, nil
}

func (c *DashdRPCClient) GetTxStatus(txid string) (bool, error) {
	details, err := c.GetTxDetails(txid)
	if err != nil {
		return false, err
	}
	return details.Confirmed, nil
}

func (c *DashdRPCClient) GetTxDetails(txid string) (TxConfirmationDetails, error) {
	tx, err := c.getRawTxVerbose(txid)
	if err != nil {
		if isRPCErrorCode(err, rpcInvalidAddrOrKey) {
			// Tx not in mempool or recent blocks (or pruned away).
			return TxConfirmationDetails{}, nil
		}
		return TxConfirmationDetails{}, err
	}

	// In mempool (no block yet) → unconfirmed. Still propagate IS-lock
	// status from either dashd's `instantlock` flag on the rawtx response
	// or our ZMQ-fed cache.
	if tx.Confirmations <= 0 || tx.BlockHash == "" {
		return TxConfirmationDetails{
			IsInstantLocked: tx.InstantLock || c.isInstantLocked(txid),
		}, nil
	}

	// Confirmed in a block — find the tx index inside the block.
	rawBlock, err := c.call("getblock", tx.BlockHash, 1) // verbosity 1 = include txids
	if err != nil {
		return TxConfirmationDetails{}, fmt.Errorf("getblock %s: %w", tx.BlockHash, err)
	}
	var block struct {
		Height uint64   `json:"height"`
		Tx     []string `json:"tx"`
	}
	if err := json.Unmarshal(rawBlock, &block); err != nil {
		return TxConfirmationDetails{}, fmt.Errorf("decode block: %w", err)
	}

	idx := uint32(0)
	found := false
	for i, id := range block.Tx {
		if id == txid {
			idx = uint32(i)
			found = true
			break
		}
	}
	if !found {
		// Should be impossible, but be loud about it rather than silently returning 0.
		slog.Warn("dashd: txid not found in its own block", "txid", txid, "blockhash", tx.BlockHash)
	}

	return TxConfirmationDetails{
		Confirmed:       true,
		BlockHeight:     block.Height,
		BlockHash:       tx.BlockHash,
		TxIndex:         idx,
		IsInstantLocked: tx.InstantLock || c.isInstantLocked(txid),
	}, nil
}

func (c *DashdRPCClient) PostTx(rawTx string) error {
	_, err := c.call("sendrawtransaction", rawTx)
	if err != nil {
		return fmt.Errorf("sendrawtransaction: %w", err)
	}
	return nil
}

// Compile-time check that we satisfy the interface.
var _ BlockchainClient = (*DashdRPCClient)(nil)
