package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"

	"github.com/decred/dcrd/dcrec/secp256k1/v2"
	"github.com/vsc-eco/hivego"
)

// secpCompactSigSize is the byte layout of a dcrd compact ECDSA signature
// (1 header byte + 32-byte R + 32-byte S). Used by IsLowS to reach the S
// component without re-parsing the whole signature.
const secpCompactSigSize = 1 + 32 + 32

// halfOrderN is N/2 of the secp256k1 group order, precomputed once. Any
// signature with S > halfOrderN is non-canonical (high-S form) and is the
// trivial malleated counterpart of a valid low-S signature.
var halfOrderN = new(big.Int).Rsh(secp256k1.S256().Params().N, 1)

// IsLowS reports whether the S component of a compact ECDSA signature is
// in the lower half of the group order. BIP-62 / RFC 6979 low-S form is
// the canonical form; rejecting high-S kills the (r, N-s) malleability
// twin that S1 exploits to double-count a single signer's gateway vote.
func IsLowS(sigBytes []byte) bool {
	if len(sigBytes) != secpCompactSigSize {
		return false
	}
	s := new(big.Int).SetBytes(sigBytes[1+32:])
	return s.Sign() > 0 && s.Cmp(halfOrderN) <= 0
}

// hasNonZeroR reports whether the R component of a compact ECDSA signature
// is non-zero. review6 H3: secp256k1.RecoverCompact returns a nil pubKey
// when R==0 (it cannot construct the inverse) and the subsequent
// `*hivego.GetPublicKeyString(pubKey)` dereference panics. Guarding R≠0
// before RecoverCompact converts the crash into an ordinary signature
// rejection, restoring liveness for the gateway collector goroutine that
// processes adversarial sign-response payloads.
func hasNonZeroR(sigBytes []byte) bool {
	if len(sigBytes) != secpCompactSigSize {
		return false
	}
	r := new(big.Int).SetBytes(sigBytes[1 : 1+32])
	return r.Sign() > 0
}

// hivePublicKeyPrefix is the Hive base58 public-key prefix. Kept in sync with
// hivego.PublicKeyPrefix; copied locally so the FUZZ-1 guard can length-check
// without round-tripping through the panicking decoder.
const hivePublicKeyPrefix = "STM"

// hivePublicKeyMinLen is a conservative lower bound on a well-formed Hive
// public key string. A genuine 33-byte compressed pubkey + 4-byte checksum
// base58-encodes to ~50 chars; we accept anything ≥ prefix+30 as "shape ok"
// and rely on hivego.DecodePublicKey (called inside the recover wrapper) to
// reject the remainder.
const hivePublicKeyMinLen = len(hivePublicKeyPrefix) + 30

// safeValidateGatewayKey wraps hivego.DecodePublicKey in a recover so that
// adversary-controlled witness GatewayKey strings (FUZZ-1) cannot crash the
// node when they are about to be serialized into a Hive multisig-rotation tx.
//
// Why: hivego.DecodePublicKey (keys.go:41) slices decoded[len-4:] without a
// length check. Inputs like "STM" or any base58 body that decodes to <4 bytes
// trigger "slice bounds out of range [-4:]" panic. That panic fires inside
// the unrecovered TickKeyRotation goroutine and takes down vsc-node entirely
// for every elected witness on the same rotation block. The upstream fix
// belongs in vsc-eco/hivego; this guard makes the node defensive even before
// that lands.
func safeValidateGatewayKey(key string) (err error) {
	if len(key) < hivePublicKeyMinLen {
		return fmt.Errorf("gateway key too short (%d chars)", len(key))
	}
	if key[:len(hivePublicKeyPrefix)] != hivePublicKeyPrefix {
		return errors.New("gateway key missing STM prefix")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("gateway key decoder panic: %v", r)
		}
	}()
	if _, decodeErr := hivego.DecodePublicKey(key); decodeErr != nil {
		return decodeErr
	}
	return nil
}

func RecoverPublicKey(signature string, hash []byte) (string, error) {
	sigBytes, err := hex.DecodeString(signature)
	if err != nil {
		return "", err
	}
	// Length is checked here so the high-S branch below can't conflate
	// "wrong byte count" with "non-canonical S" in operator logs.
	if len(sigBytes) != secpCompactSigSize {
		return "", fmt.Errorf("invalid compact signature length: got %d, want %d", len(sigBytes), secpCompactSigSize)
	}
	// S1: reject the high-S malleated twin of any valid signature before
	// pubkey recovery. Without this, (r, N-s) recovers the same pubkey but
	// presents a different sig string, defeating the dedup at collectSigs.
	if !IsLowS(sigBytes) {
		return "", errors.New("signature has non-canonical high-S value")
	}
	// review6 H3: reject R==0 BEFORE RecoverCompact. dcrd's RecoverCompact
	// returns (nil, false, err) on R==0 but several call paths (notably the
	// gateway collector goroutine in multisig.go) historically ignored err
	// and dereferenced pubKey, producing an unrecovered panic that crashed
	// the node on a single adversarial pubsub message. Even with proper err
	// handling downstream this is still defense-in-depth — any future caller
	// that copy-pastes the recovery pattern is now safe by construction.
	if !hasNonZeroR(sigBytes) {
		return "", errors.New("signature has zero R value (non-recoverable)")
	}
	pubKey, _, err := secp256k1.RecoverCompact(sigBytes, hash)
	if err != nil {
		return "", err
	}
	// Belt-and-suspenders nil check: a non-error return path that still
	// produces nil pubKey would otherwise panic on the next line. Should
	// never trigger given the IsLowS + R≠0 guards above, but cheap to keep.
	if pubKey == nil {
		return "", errors.New("RecoverCompact returned nil pubkey without error")
	}
	return *hivego.GetPublicKeyString(pubKey), nil
}

func GatewayKeyFromBlsSeed(blsSeed string) (*hivego.KeyPair, error) {
	blsPrivSeed, err := hex.DecodeString(blsSeed)
	if err != nil {
		return nil, err
	}
	salt := []byte("gateway_key")
	gatewayKey := sha256.Sum256(append(blsPrivSeed, salt...))

	kp := hivego.KeyPairFromBytes(gatewayKey[:])
	return kp, nil
}
