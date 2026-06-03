package gateway

// review6 H3: RecoverPublicKey R==0 nil-deref guard.
//
// secp256k1.RecoverCompact returns (nil, false, err) when the signature's R
// component is zero — but several historical call paths dereferenced the
// returned *PublicKey before checking err, producing an unrecovered panic on
// a single adversarial pubsub message. utils.go now:
//
//   1. exposes hasNonZeroR(sigBytes) as the canonical, length-checked predicate;
//   2. rejects R==0 in RecoverPublicKey BEFORE calling RecoverCompact;
//   3. belt-and-suspenders nil-check the returned pubKey for defense in depth.
//
// This file exercises those three guarantees directly.

import (
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	"github.com/decred/dcrd/dcrec/secp256k1/v2"
)

// TestReview6_H3_HasNonZeroR_ZeroR asserts that a 65-byte compact sig with an
// all-zero R component is rejected by the length-correct predicate. This is the
// adversarial precondition that triggered the historic crash.
func TestReview6_H3_HasNonZeroR_ZeroR(t *testing.T) {
	sig := make([]byte, secpCompactSigSize)
	sig[0] = 27 // header byte: iter=0, compressedFlag=0, low 27 base
	// bytes [1:33] = R, leave as zero
	// bytes [33:65] = S, set to non-zero ones so the only failure mode is R==0
	for i := 1 + 32; i < secpCompactSigSize; i++ {
		sig[i] = 0x01
	}
	if hasNonZeroR(sig) {
		t.Fatalf("hasNonZeroR(R=0) = true, want false")
	}
}

// TestReview6_H3_HasNonZeroR_NonZeroR confirms the predicate accepts a sig
// with R=1 — the smallest possible non-zero R.
func TestReview6_H3_HasNonZeroR_NonZeroR(t *testing.T) {
	sig := make([]byte, secpCompactSigSize)
	sig[0] = 27
	sig[1+31] = 0x01 // R = 1 (big-endian)
	sig[1+32+31] = 0x01
	if !hasNonZeroR(sig) {
		t.Fatalf("hasNonZeroR(R=1) = false, want true")
	}
}

// TestReview6_H3_HasNonZeroR_WrongLength asserts the predicate refuses to
// inspect a sig that doesn't have the secp compact 1+32+32 layout. Length
// mismatch must short-circuit to false; otherwise the slice indexing inside
// would either panic or read attacker-controlled bytes from a neighbouring
// allocation.
func TestReview6_H3_HasNonZeroR_WrongLength(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		make([]byte, 1),
		make([]byte, 32),
		make([]byte, secpCompactSigSize-1),
		make([]byte, secpCompactSigSize+1),
	}
	for _, sig := range cases {
		if hasNonZeroR(sig) {
			t.Fatalf("hasNonZeroR(len=%d) = true, want false", len(sig))
		}
	}
}

// TestReview6_H3_RecoverPublicKey_R0_ErrorsNoPanic crafts the exact adversarial
// frame (header byte 27, R=0, S=non-zero) the H3 audit identified and asserts
// the function returns a *named* error rather than panicking. The previous
// behaviour was a nil-pointer dereference inside the *hivego.GetPublicKeyString
// dereference at the bottom of RecoverPublicKey; the test would crash instead
// of failing if the guard regresses.
func TestReview6_H3_RecoverPublicKey_R0_ErrorsNoPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("RecoverPublicKey panicked on R=0 sig: %v — guard regression", r)
		}
	}()

	sig := make([]byte, secpCompactSigSize)
	sig[0] = 27
	// R = 0 (bytes 1..33 left zero), S = 1 (last byte non-zero)
	sig[secpCompactSigSize-1] = 0x01
	sigHex := hex.EncodeToString(sig)

	hash := sha256.Sum256([]byte("review6 H3 — zero-R adversarial frame"))
	_, err := RecoverPublicKey(sigHex, hash[:])
	if err == nil {
		t.Fatalf("RecoverPublicKey(R=0) returned no error — guard missing")
	}
	if !strings.Contains(err.Error(), "zero R") {
		t.Fatalf("RecoverPublicKey error %q does not mention 'zero R'", err.Error())
	}
}

// TestReview6_H3_RecoverPublicKey_ValidSigRoundtrip is the regression
// counterpart: the new R==0 guard must NOT break the happy path. We sign a
// hash with a deterministic seed, normalise to low-S (since SignCompact does
// not guarantee low-S form), and assert RecoverPublicKey accepts it and
// returns a non-empty STM-prefixed pubkey string.
func TestReview6_H3_RecoverPublicKey_ValidSigRoundtrip(t *testing.T) {
	seed := sha256.Sum256([]byte("review6-H3-roundtrip-seed"))
	prvKey, _ := secp256k1.PrivKeyFromBytes(seed[:])

	msgHash := sha256.Sum256([]byte("review6 H3 valid signature roundtrip"))
	sigBytes, err := secp256k1.SignCompact(prvKey, msgHash[:], true)
	if err != nil {
		t.Fatalf("SignCompact: %v", err)
	}

	// Normalise to low-S if necessary — same pattern audit_unfixed_internal_test
	// uses to keep the canonical-side acceptable to RecoverPublicKey's IsLowS.
	N := secp256k1.S256().Params().N
	bitlen := (secp256k1.S256().BitSize + 7) / 8
	if !IsLowS(sigBytes) {
		canon := make([]byte, len(sigBytes))
		copy(canon, sigBytes)
		header := canon[0] - 27
		compressedFlag := header & 4
		iter := header & ^byte(4)
		canon[0] = 27 + ((iter ^ 1) | compressedFlag)
		S := new(big.Int).SetBytes(canon[1+bitlen:])
		negS := new(big.Int).Sub(N, S)
		negSBytes := negS.Bytes()
		padded := make([]byte, bitlen)
		copy(padded[bitlen-len(negSBytes):], negSBytes)
		copy(canon[1+bitlen:], padded)
		sigBytes = canon
	}

	pub, err := RecoverPublicKey(hex.EncodeToString(sigBytes), msgHash[:])
	if err != nil {
		t.Fatalf("RecoverPublicKey on valid low-S sig: %v", err)
	}
	if !strings.HasPrefix(pub, "STM") {
		t.Fatalf("recovered pubkey %q lacks STM prefix", pub)
	}
}
