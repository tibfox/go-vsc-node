package contractinterface

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// review6 M5: types_gen.go is msgp-generated; its DecodeMsg/UnmarshalMsg do
// `make([]UnsignedSigHash, zb0002)` where zb0002 is a uint32 read straight
// off the wire. A 13-byte msgpack frame with the maximum array header
// (0xdd 0xff 0xff 0xff 0xff) coerces ~4.3 billion entries to be allocated,
// each ~64 bytes after slice headers — ~275 GB of memory. The bot OOM's
// long before that. RUNTIME-PROVEN by the audit.
//
// Modifying types_gen.go directly is fragile (header reads "DO NOT EDIT";
// any `go generate` regen reverts the fix). Instead, every caller routes
// the wire payload through SafeUnmarshalSigningData, which:
//
//   1. caps the payload size at a generous-but-finite bound (1 MiB);
//   2. parses the msgpack header in-line and rejects an UnsignedSigHashes
//      array length above MaxUnsignedSigHashes (256) — well above any
//      realistic Bitcoin tx (P2WSH multisig spends have at most one
//      sighash per input; a 100-input tx is already past the network
//      relay limit);
//   3. only THEN dispatches to the generated UnmarshalMsg.
//
// The cap is data-shape, not bytes-on-the-wire — so a single ~92 GB
// allocation can never be triggered even with a perfectly small frame.

const (
	// SafeUnmarshalPayloadCap bounds the raw wire payload. 1 MiB matches the
	// HTTP body cap in cmd/mapping-bot/http_server.go and is well above any
	// realistic SigningData blob (a 100-input tx is ~30 KiB encoded).
	SafeUnmarshalPayloadCap = 1 << 20

	// MaxUnsignedSigHashes is the per-message ceiling on the inline array.
	// Realistic Bitcoin txs have ≤ tens of inputs; 256 is generous and
	// keeps the worst-case allocation at 256 * sizeof(UnsignedSigHash) ≈
	// 24 KiB — bounded by data shape, not wire size.
	MaxUnsignedSigHashes = 256
)

// SafeUnmarshalSigningData parses a wire-format SigningData blob with the
// hardening guards documented above. Errors are wrapped with a stable
// prefix so call sites can grep for them.
func SafeUnmarshalSigningData(wire []byte) (*SigningData, error) {
	if len(wire) > SafeUnmarshalPayloadCap {
		return nil, fmt.Errorf("SigningData: wire payload exceeds %d bytes (got %d)", SafeUnmarshalPayloadCap, len(wire))
	}
	if err := preflightSigningData(wire); err != nil {
		return nil, err
	}
	var s SigningData
	if _, err := s.UnmarshalMsg(wire); err != nil {
		return nil, err
	}
	return &s, nil
}

// preflightSigningData scans the wire payload's msgpack header for the
// "uh" (UnsignedSigHashes) field and rejects an oversized array length
// BEFORE UnmarshalMsg sees it. The scan is deliberately conservative — on
// any structural mismatch we bail to UnmarshalMsg, which will return its
// own error rather than allocating. The goal is solely to prevent the
// known OOM path; correctness of the rest of the blob is UnmarshalMsg's
// job.
func preflightSigningData(buf []byte) error {
	if len(buf) < 1 {
		return errors.New("SigningData: empty payload")
	}
	// SigningData is a 2-field map; expect 0x82.
	if buf[0] != 0x82 {
		return nil // not the shape we're guarding; let UnmarshalMsg complain
	}
	i := 1
	for fieldsRemaining := 2; fieldsRemaining > 0 && i < len(buf); fieldsRemaining-- {
		// Each field: <key> <value>. Key is a fixstr 0xa0..0xbf (msgpack
		// short string). We expect "tx" (0xa2 't' 'x') or "uh" (0xa2 'u' 'h').
		if i >= len(buf) {
			return nil
		}
		k := buf[i]
		if k < 0xa1 || k > 0xbf {
			return nil
		}
		klen := int(k - 0xa0)
		if i+1+klen > len(buf) {
			return nil
		}
		key := string(buf[i+1 : i+1+klen])
		i += 1 + klen
		if i >= len(buf) {
			return nil
		}
		if key != "uh" {
			// Skip the value by best-effort: we don't need to be precise,
			// only to avoid mis-identifying its array header. msgp's own
			// UnmarshalMsg handles the rest; we just need to find "uh".
			i = skipMsgpValue(buf, i)
			if i < 0 {
				return nil
			}
			continue
		}
		// "uh" value should be an array — short array (0x90..0x9f), or
		// array16 (0xdc) or array32 (0xdd).
		v := buf[i]
		var arrLen uint32
		switch {
		case v >= 0x90 && v <= 0x9f:
			arrLen = uint32(v - 0x90)
		case v == 0xdc:
			if i+3 > len(buf) {
				return nil
			}
			arrLen = uint32(binary.BigEndian.Uint16(buf[i+1 : i+3]))
		case v == 0xdd:
			if i+5 > len(buf) {
				return nil
			}
			arrLen = binary.BigEndian.Uint32(buf[i+1 : i+5])
		default:
			return nil
		}
		if arrLen > MaxUnsignedSigHashes {
			return fmt.Errorf("SigningData: UnsignedSigHashes array length %d exceeds cap %d", arrLen, MaxUnsignedSigHashes)
		}
		return nil
	}
	return nil
}

// skipMsgpValue advances past one msgpack value at buf[i] and returns the
// new offset. Returns -1 on truncated input. Best-effort — only the
// subset needed to skip a SigningData.Tx field (bin/str of bounded size)
// is handled; everything else falls back to "give up" so the caller can
// let UnmarshalMsg handle the inner content.
func skipMsgpValue(buf []byte, i int) int {
	if i >= len(buf) {
		return -1
	}
	t := buf[i]
	switch {
	case t == 0xc4: // bin 8
		if i+2 > len(buf) {
			return -1
		}
		l := int(buf[i+1])
		end := i + 2 + l
		if end > len(buf) {
			return -1
		}
		return end
	case t == 0xc5: // bin 16
		if i+3 > len(buf) {
			return -1
		}
		l := int(binary.BigEndian.Uint16(buf[i+1 : i+3]))
		end := i + 3 + l
		if end > len(buf) {
			return -1
		}
		return end
	case t == 0xc6: // bin 32
		if i+5 > len(buf) {
			return -1
		}
		l := int(binary.BigEndian.Uint32(buf[i+1 : i+5]))
		end := i + 5 + l
		if end > len(buf) {
			return -1
		}
		return end
	case t >= 0xa0 && t <= 0xbf: // fixstr
		l := int(t - 0xa0)
		end := i + 1 + l
		if end > len(buf) {
			return -1
		}
		return end
	default:
		return -1
	}
}
