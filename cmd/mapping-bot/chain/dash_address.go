package chain

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/txscript"
)

// DashAddressGenerator implements AddressGenerator for Dash.
//
// It builds the SAME primary/backup-with-CSV redeem script as
// BTCAddressGenerator, but wraps it as P2SH (HASH160 → base58) instead of
// P2WSH (SHA256 → bech32). Dash Core never adopted SegWit — its address
// parser rejects bech32 across all networks (mainnet, testnet, regtest),
// so a P2WSH deposit address can't be used from a real Dash wallet.
// P2SH base58 addresses (prefix `7…` on mainnet, `8…/9…` on testnet/regtest
// per dash-mapping-contract's address-version overrides) work everywhere.
type DashAddressGenerator struct {
	Params          *chaincfg.Params
	BackupCSVBlocks int
}

func (g *DashAddressGenerator) GenerateDepositAddress(
	primaryPubKeyHex, backupPubKeyHex, instruction string,
) (string, []byte, error) {
	primaryPubKey, err := hex.DecodeString(primaryPubKeyHex)
	if err != nil {
		return "", nil, err
	}
	backupPubKey, err := hex.DecodeString(backupPubKeyHex)
	if err != nil {
		return "", nil, err
	}

	tag := sha256.Sum256([]byte(instruction))
	return createP2SHWithBackup(primaryPubKey, backupPubKey, tag[:], g.Params, g.BackupCSVBlocks)
}

// createP2SHWithBackup builds a P2SH address with a primary key path and
// a backup key path gated by OP_CHECKSEQUENCEVERIFY. The redeem script
// matches dash-mapping-contract/contract/mapping/utils.go::createP2SHAddressWithBackup
// byte-for-byte.
func createP2SHWithBackup(
	primaryPubKey, backupPubKey, tag []byte,
	network *chaincfg.Params,
	csvBlocks int,
) (string, []byte, error) {
	sb := txscript.NewScriptBuilder()

	sb.AddOp(txscript.OP_IF)

	sb.AddData(primaryPubKey)
	if tag != nil && len(tag) > 0 {
		sb.AddOp(txscript.OP_CHECKSIGVERIFY)
		sb.AddData(tag)
	} else {
		sb.AddOp(txscript.OP_CHECKSIG)
	}

	sb.AddOp(txscript.OP_ELSE)
	sb.AddInt64(int64(csvBlocks))
	sb.AddOp(txscript.OP_CHECKSEQUENCEVERIFY)
	sb.AddOp(txscript.OP_DROP)
	sb.AddData(backupPubKey)
	sb.AddOp(txscript.OP_CHECKSIG)

	sb.AddOp(txscript.OP_ENDIF)

	redeemScript, err := sb.Script()
	if err != nil {
		return "", nil, err
	}

	addr, err := btcutil.NewAddressScriptHash(redeemScript, network)
	if err != nil {
		return "", nil, err
	}
	return addr.EncodeAddress(), redeemScript, nil
}
