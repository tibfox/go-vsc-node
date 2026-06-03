package contractinterface

// review6 M5: SafeUnmarshalSigningData preflight DoS guard.
//
// The msgp-generated UnmarshalMsg does `make([]UnsignedSigHash, zb0002)` where
// zb0002 is a uint32 read straight off the wire — a 13-byte frame with
// arrayN-length 0xffffffff coerces ~4.3B entries (~275 GB) to be allocated.
//
// The fix (safe_decode.go) preflights every wire payload through a hand-rolled
// msgpack scanner that rejects oversized UnsignedSigHashes arrays AND caps the
// raw payload at 1 MiB. This file drives:
//
//   - the happy path (a tiny, valid 0x82-headed frame round-trips);
//   - the original attack (0x82 + 0xdd 0xffffffff);
//   - the audit-correction bypass attempts (0x83 with extra k/v, and a
//     0xde map16 with 2 fields);
//   - the 1 MiB raw-payload cap.

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// hugeArrayHeader builds the four-byte big-endian array32 header for an array
// of length n (preceded by the 0xdd marker byte).
func hugeArrayHeader(n uint32) []byte {
	hdr := make([]byte, 5)
	hdr[0] = 0xdd
	binary.BigEndian.PutUint32(hdr[1:], n)
	return hdr
}

// TestReview6_M5_SafeUnmarshal_ValidFrameRoundtrip asserts the preflight does
// NOT regress canonical frames. We use the msgp-generated MarshalMsg so the
// happy-path payload is exactly what production writes.
func TestReview6_M5_SafeUnmarshal_ValidFrameRoundtrip(t *testing.T) {
	orig := &SigningData{
		Tx: []byte{0xde, 0xad, 0xbe, 0xef},
		UnsignedSigHashes: []UnsignedSigHash{
			{Index: 0, SigHash: []byte{0x01, 0x02}, WitnessScript: []byte{0x03}},
			{Index: 1, SigHash: []byte{0x04, 0x05}, WitnessScript: []byte{0x06}},
		},
	}
	wire, err := orig.MarshalMsg(nil)
	if err != nil {
		t.Fatalf("MarshalMsg: %v", err)
	}

	got, err := SafeUnmarshalSigningData(wire)
	if err != nil {
		t.Fatalf("SafeUnmarshalSigningData on valid frame: %v", err)
	}
	if !bytes.Equal(got.Tx, orig.Tx) {
		t.Fatalf("Tx mismatch: got %x, want %x", got.Tx, orig.Tx)
	}
	if len(got.UnsignedSigHashes) != len(orig.UnsignedSigHashes) {
		t.Fatalf("UnsignedSigHashes len = %d, want %d", len(got.UnsignedSigHashes), len(orig.UnsignedSigHashes))
	}
}

// TestReview6_M5_SafeUnmarshal_Rejects0x82WithHugeUH drives the exact CVE
// payload — a 2-field fixmap header (0x82) where the "uh" entry declares an
// array32 of length 0xffffffff. Preflight MUST reject with the "exceeds cap"
// substring BEFORE UnmarshalMsg sees the giant length.
func TestReview6_M5_SafeUnmarshal_Rejects0x82WithHugeUH(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(0x82)                  // fixmap, 2 fields
	buf.Write([]byte{0xa2, 't', 'x'})    // key "tx"
	buf.Write([]byte{0xc4, 0x00})        // bin8 with 0 bytes — minimal valid Tx
	buf.Write([]byte{0xa2, 'u', 'h'})    // key "uh"
	buf.Write(hugeArrayHeader(0xffffffff))

	_, err := SafeUnmarshalSigningData(buf.Bytes())
	if err == nil {
		t.Fatalf("expected rejection of 0x82 + huge uh, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("err %q does not mention 'exceeds cap'", err.Error())
	}
}

// TestReview6_M5_SafeUnmarshal_Rejects0x83Bypass is the audit-correction
// counter-example: the prior preflight only matched 0x82 exactly. An attacker
// can use 0x83 (fixmap, 3 fields) with one extra dummy k/v and still get the
// huge "uh" through the generated decoder. The corrected preflight handles
// every fixmap variant; this test asserts the 0x83 path is also rejected.
func TestReview6_M5_SafeUnmarshal_Rejects0x83Bypass(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(0x83) // fixmap, 3 fields
	// One extra k/v BEFORE "uh": key "xx", value fixstr "v" (skipMsgpValue
	// can step past fixstr deterministically).
	buf.Write([]byte{0xa2, 'x', 'x'})
	buf.Write([]byte{0xa1, 'v'})
	// Standard tx field
	buf.Write([]byte{0xa2, 't', 'x'})
	buf.Write([]byte{0xc4, 0x00})
	// Adversarial uh
	buf.Write([]byte{0xa2, 'u', 'h'})
	buf.Write(hugeArrayHeader(0xffffffff))

	_, err := SafeUnmarshalSigningData(buf.Bytes())
	if err == nil {
		t.Fatalf("expected rejection of 0x83 bypass, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("err %q does not mention 'exceeds cap'", err.Error())
	}
}

// TestReview6_M5_SafeUnmarshal_RejectsMap16Bypass exercises the 0xde (map16)
// variant — a 16-bit map header that the original preflight ignored. With 2
// declared fields and a huge "uh" inside, this also must be rejected.
func TestReview6_M5_SafeUnmarshal_RejectsMap16Bypass(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(0xde)               // map16
	buf.Write([]byte{0x00, 0x02})     // 2 fields
	buf.Write([]byte{0xa2, 't', 'x'}) // tx key
	buf.Write([]byte{0xc4, 0x00})     // empty tx bin
	buf.Write([]byte{0xa2, 'u', 'h'}) // uh key
	buf.Write(hugeArrayHeader(0xffffffff))

	_, err := SafeUnmarshalSigningData(buf.Bytes())
	if err == nil {
		t.Fatalf("expected rejection of map16 bypass, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("err %q does not mention 'exceeds cap'", err.Error())
	}
}

// TestReview6_M5_SafeUnmarshal_RejectsOversizedPayload asserts the outermost
// 1 MiB raw cap fires before preflight even starts scanning. A 2 MiB blob
// won't reach UnmarshalMsg or the msgpack scanner — it's rejected at the
// length check.
func TestReview6_M5_SafeUnmarshal_RejectsOversizedPayload(t *testing.T) {
	wire := make([]byte, 2*1024*1024)
	wire[0] = 0x82 // headed like a valid frame, but too large to matter
	_, err := SafeUnmarshalSigningData(wire)
	if err == nil {
		t.Fatalf("expected rejection of 2 MiB payload, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("err %q does not mention 'exceeds'", err.Error())
	}
}

// TestReview6_M5_SafeUnmarshal_AcceptsBoundaryUHArray confirms the upper edge
// of the data-shape cap (exactly MaxUnsignedSigHashes entries) is allowed —
// the preflight is "> MaxUnsignedSigHashes", not ">=". An over-strict guard
// here would silently kill legitimate large-input batches.
func TestReview6_M5_SafeUnmarshal_AcceptsBoundaryUHArray(t *testing.T) {
	orig := &SigningData{
		Tx:                []byte{0x00},
		UnsignedSigHashes: make([]UnsignedSigHash, MaxUnsignedSigHashes),
	}
	for i := range orig.UnsignedSigHashes {
		orig.UnsignedSigHashes[i] = UnsignedSigHash{Index: uint32(i), SigHash: []byte{0x01}, WitnessScript: []byte{0x02}}
	}
	wire, err := orig.MarshalMsg(nil)
	if err != nil {
		t.Fatalf("MarshalMsg: %v", err)
	}
	if got, err := SafeUnmarshalSigningData(wire); err != nil {
		t.Fatalf("SafeUnmarshalSigningData at boundary: %v", err)
	} else if len(got.UnsignedSigHashes) != MaxUnsignedSigHashes {
		t.Fatalf("boundary frame round-tripped with %d entries, want %d", len(got.UnsignedSigHashes), MaxUnsignedSigHashes)
	}
}

// TestReview6_M5_SafeUnmarshal_RejectsAboveBoundaryUH asserts MaxUnsignedSigHashes+1
// is rejected. This is the smallest unambiguously-over-cap input — the preflight
// must catch it without falling back to UnmarshalMsg (which would happily
// allocate the extra slot).
func TestReview6_M5_SafeUnmarshal_RejectsAboveBoundaryUH(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(0x82)
	buf.Write([]byte{0xa2, 't', 'x'})
	buf.Write([]byte{0xc4, 0x00})
	buf.Write([]byte{0xa2, 'u', 'h'})
	// array32 with MaxUnsignedSigHashes+1 entries
	hdr := make([]byte, 5)
	hdr[0] = 0xdd
	binary.BigEndian.PutUint32(hdr[1:], uint32(MaxUnsignedSigHashes+1))
	buf.Write(hdr)

	_, err := SafeUnmarshalSigningData(buf.Bytes())
	if err == nil {
		t.Fatalf("expected rejection at MaxUnsignedSigHashes+1, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("err %q does not mention 'exceeds cap'", err.Error())
	}
}
