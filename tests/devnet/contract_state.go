package devnet

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// QueryContractState fetches the raw value of a single contract state key
// via the magi node's GraphQL API (getStateByKeys with hex encoding).
// Returns the value as a []byte (which is how the contract stores binary
// state).
//
// On a state-not-found response, returns (nil, nil) — callers must
// distinguish "no entry" from "empty entry" themselves.
func (d *Devnet) QueryContractState(ctx context.Context, node int, contractId, key string) ([]byte, error) {
	endpoint := d.GQLEndpoint(node)
	query := map[string]any{
		"query": `query($cid: String!, $keys: [String!]!) {
			getStateByKeys(contractId: $cid, keys: $keys, encoding: "hex")
		}`,
		"variables": map[string]any{
			"cid":  contractId,
			"keys": []string{key},
		},
	}
	body, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint,
		bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("graphql request: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("graphql read: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("graphql status %d: %s", resp.StatusCode, string(respBody))
	}

	var parsed struct {
		Data struct {
			GetStateByKeys map[string]any `json:"getStateByKeys"`
		} `json:"data"`
		Errors []any `json:"errors,omitempty"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("graphql decode: %w (body=%s)", err, string(respBody))
	}
	if len(parsed.Errors) > 0 {
		return nil, fmt.Errorf("graphql errors: %v", parsed.Errors)
	}
	if parsed.Data.GetStateByKeys == nil {
		return nil, nil
	}
	raw, ok := parsed.Data.GetStateByKeys[key]
	if !ok || raw == nil {
		return nil, nil
	}
	s, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected state-value type %T (want string of hex)", raw)
	}
	if s == "" {
		return nil, nil
	}
	// `encoding: "hex"` returns hex-encoded bytes.
	bs, err := hexDecodeFlexible(s)
	if err != nil {
		return nil, fmt.Errorf("decode state hex %q: %w", s, err)
	}
	return bs, nil
}

// hexDecodeFlexible accepts either "0x..." or bare hex, and returns the
// raw bytes. Empty string → empty slice.
func hexDecodeFlexible(s string) ([]byte, error) {
	if len(s) >= 2 && s[0:2] == "0x" {
		s = s[2:]
	}
	if s == "" {
		return nil, nil
	}
	return hexBytes(s)
}

func hexBytes(s string) ([]byte, error) {
	b := make([]byte, len(s)/2)
	for i := 0; i < len(b); i++ {
		hi, err := hexNib(s[2*i])
		if err != nil {
			return nil, err
		}
		lo, err := hexNib(s[2*i+1])
		if err != nil {
			return nil, err
		}
		b[i] = (hi << 4) | lo
	}
	return b, nil
}

func hexNib(c byte) (byte, error) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', nil
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, nil
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, nil
	}
	return 0, fmt.Errorf("bad hex char %q", c)
}

// WaitForContractState polls QueryContractState until either:
//   - check(value) returns true (success), or
//   - timeout elapses (returns the last value seen + the last error).
//
// Useful for asserting state after a custom_json that may take a Hive
// block to land.
func (d *Devnet) WaitForContractState(
	ctx context.Context,
	node int,
	contractId, key string,
	timeout time.Duration,
	check func(value []byte) bool,
) ([]byte, error) {
	deadline := time.Now().Add(timeout)
	var (
		last    []byte
		lastErr error
	)
	for {
		v, err := d.QueryContractState(ctx, node, contractId, key)
		last = v
		lastErr = err
		if err == nil && check(v) {
			return v, nil
		}
		if time.Now().After(deadline) {
			return last, fmt.Errorf("timed out waiting for contract state: lastErr=%v lastValue=%x",
				lastErr, last)
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// DecodeContractBalance decodes the contract's compact big-endian balance
// encoding into a duff (satoshi) amount. Empty input means zero.
//
// The contract encodes balances by trimming leading zero bytes from a
// big-endian uint64 (see setAccBal in mapping.go).
func DecodeContractBalance(raw []byte) int64 {
	if len(raw) == 0 {
		return 0
	}
	if len(raw) > 8 {
		return -1 // overflow / corrupt
	}
	var buf [8]byte
	copy(buf[8-len(raw):], raw)
	return int64(binary.BigEndian.Uint64(buf[:]))
}

// tryBase64Decode best-effort decodes s as base64. Returns nil if s
// doesn't look like base64 (e.g. it's a raw-byte string from an older
// magi schema).
func tryBase64Decode(s string) []byte {
	// quick reject for raw-byte-looking strings
	if strings.ContainsAny(s, "\x00\x01\x02\x03\x04\x05\x06\x07\x08") {
		return nil
	}
	// reject strings with whitespace/punctuation that aren't part of std base64
	for _, r := range s {
		if r == '=' || r == '+' || r == '/' || r == '_' || r == '-' {
			continue
		}
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			continue
		}
		return nil
	}
	// Try base64 decode (std + url-safe)
	for _, dec := range []func(string) ([]byte, error){
		base64StdDecode, base64URLDecode,
	} {
		if b, err := dec(s); err == nil {
			return b
		}
	}
	return nil
}

func base64StdDecode(s string) ([]byte, error) { return base64DecodePadded(s, false) }
func base64URLDecode(s string) ([]byte, error) { return base64DecodePadded(s, true) }

func base64DecodePadded(s string, urlSafe bool) ([]byte, error) {
	if urlSafe {
		s = strings.NewReplacer("-", "+", "_", "/").Replace(s)
	}
	for len(s)%4 != 0 {
		s += "="
	}
	return base64.StdEncoding.DecodeString(s)
}
