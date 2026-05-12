package devnet

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// DashdRPCURL returns the URL the magi nodes use to talk to dashd from
// inside the docker network. Embeds the regtest user/pass declared in
// docker-compose.yml.
func (d *Devnet) DashdRPCURL() string {
	return "http://vsc-node-user:vsc-node-pass@dashd:19898"
}

// DashdRPCHostPort returns just the "host:port" part of the dashd RPC
// URL. This is what goes into the oracle's per-node Chains config
// (modules/oracle/config.go ChainRpcConfig.RpcHost).
func (d *Devnet) DashdRPCHostPort() string {
	return "dashd:19898"
}

// dashCli runs `dash-cli` inside the dashd container with the regtest
// credentials and returns the trimmed stdout.
func (d *Devnet) dashCli(ctx context.Context, args ...string) (string, error) {
	full := append([]string{
		"exec", "dashd",
		"dash-cli", "-regtest",
		"-rpcuser=vsc-node-user", "-rpcpassword=vsc-node-pass",
	}, args...)
	out, err := d.composeOutput(ctx, full...)
	return strings.TrimSpace(out), err
}

// MineDashBlocks mines `count` regtest blocks against a freshly generated
// regtest address. Returns the new tip height. Requires EnableDashd=true
// on the devnet config.
func (d *Devnet) MineDashBlocks(ctx context.Context, count int) (uint64, error) {
	if count < 1 {
		return 0, fmt.Errorf("count must be >= 1")
	}

	// Create wallet if missing. Ignore "already exists" error.
	_, _ = d.dashCli(ctx, "-named", "createwallet", "wallet_name=devnet", "load_on_startup=true")

	addr, err := d.dashCli(ctx, "getnewaddress")
	if err != nil {
		return 0, fmt.Errorf("getnewaddress: %w", err)
	}

	if _, err := d.dashCli(ctx, "generatetoaddress", fmt.Sprint(count), addr); err != nil {
		return 0, fmt.Errorf("generatetoaddress: %w", err)
	}

	return d.DashHeight(ctx)
}

// DashHeight returns the dashd regtest tip height.
func (d *Devnet) DashHeight(ctx context.Context) (uint64, error) {
	out, err := d.dashCli(ctx, "getblockcount")
	if err != nil {
		return 0, fmt.Errorf("getblockcount: %w", err)
	}
	var h uint64
	if _, err := fmt.Sscanf(out, "%d", &h); err != nil {
		return 0, fmt.Errorf("parsing block count %q: %w", out, err)
	}
	return h, nil
}

// GetDashBlockHeaderHex returns the 80-byte raw block header at `height` as
// a lowercase hex string (160 chars), suitable for passing to the Dash
// mapping contract's seedBlocks / addBlocks actions.
func (d *Devnet) GetDashBlockHeaderHex(ctx context.Context, height uint64) (string, error) {
	hash, err := d.dashCli(ctx, "getblockhash", fmt.Sprint(height))
	if err != nil {
		return "", fmt.Errorf("getblockhash %d: %w", height, err)
	}
	// getblockheader <hash> false → raw 80-byte hex (verbose=true returns JSON)
	hdr, err := d.dashCli(ctx, "getblockheader", hash, "false")
	if err != nil {
		return "", fmt.Errorf("getblockheader %s: %w", hash, err)
	}
	if len(hdr) != 160 {
		return "", fmt.Errorf("unexpected header hex length %d (want 160): %q", len(hdr), hdr)
	}
	return hdr, nil
}

// WaitForDashHeight blocks until the dashd regtest tip is at least `target`
// blocks tall, or the timeout expires.
func (d *Devnet) WaitForDashHeight(ctx context.Context, target uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		h, err := d.DashHeight(ctx)
		if err == nil && h >= target {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("dashd never reached height %d (last err: %v)", target, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// EnsureDashCoinbaseMature mines enough blocks for the first coinbase to
// be spendable. Dash regtest matures coinbase after 100 confirmations,
// matching Bitcoin. We mine 105 to give a small buffer for fee math.
// Returns the new tip height.
func (d *Devnet) EnsureDashCoinbaseMature(ctx context.Context) (uint64, error) {
	h, err := d.DashHeight(ctx)
	if err != nil {
		return 0, fmt.Errorf("DashHeight: %w", err)
	}
	const target = uint64(105)
	if h >= target {
		return h, nil
	}
	return d.MineDashBlocks(ctx, int(target-h))
}

// SendDashToAddress sends `amount` (string DASH, e.g. "0.01") to `addr`
// from the dashd regtest wallet. Returns the broadcast txid. Requires a
// funded wallet — call EnsureDashCoinbaseMature first.
func (d *Devnet) SendDashToAddress(ctx context.Context, addr, amount string) (string, error) {
	out, err := d.dashCli(ctx, "sendtoaddress", addr, amount)
	if err != nil {
		return "", fmt.Errorf("sendtoaddress %s %s: %w", addr, amount, err)
	}
	return out, nil
}

// GetDashBlockHashAtHeight returns the block hash at the given height.
func (d *Devnet) GetDashBlockHashAtHeight(ctx context.Context, height uint64) (string, error) {
	return d.dashCli(ctx, "getblockhash", fmt.Sprint(height))
}

// GetDashRawBlockHex returns the raw block bytes (hex) for a given hash.
// Verbosity 0 = serialized hex.
func (d *Devnet) GetDashRawBlockHex(ctx context.Context, blockHash string) (string, error) {
	return d.dashCli(ctx, "getblock", blockHash, "0")
}

// GetDashRawTransactionHex returns the raw serialized hex of a tx.
// `blockHashHint` may be empty for mempool / -txindex paths; on a pruned
// node we don't run, we'd need to pass it.
func (d *Devnet) GetDashRawTransactionHex(ctx context.Context, txid, blockHashHint string) (string, error) {
	args := []string{"getrawtransaction", txid}
	if blockHashHint != "" {
		args = append(args, "false", blockHashHint)
	}
	return d.dashCli(ctx, args...)
}

// GetDashTxConfirmationHeight returns the block height the tx was mined
// into. Useful right after sendtoaddress + MineDashBlocks(1).
func (d *Devnet) GetDashTxConfirmationHeight(ctx context.Context, txid string) (uint64, error) {
	// `getrawtransaction <txid> 1` returns JSON with `blockhash` then
	// `getblock <hash> 1` gives the height. Simpler path: scan the tip
	// backwards until we find a block containing the txid; cheap in regtest
	// because we control mining.
	tip, err := d.DashHeight(ctx)
	if err != nil {
		return 0, fmt.Errorf("DashHeight: %w", err)
	}
	for h := tip; h > 0 && h+50 > tip; h-- {
		hash, err := d.GetDashBlockHashAtHeight(ctx, h)
		if err != nil {
			continue
		}
		// `getblock <hash> 1` lists the txids in the block.
		raw, err := d.dashCli(ctx, "getblock", hash, "1")
		if err != nil {
			continue
		}
		// Trivial substring match — txid is unique, no quotes around the
		// strings we care about would also contain it.
		if strings.Contains(raw, txid) {
			return h, nil
		}
	}
	return 0, fmt.Errorf("txid %s not found in last 50 blocks", txid)
}
