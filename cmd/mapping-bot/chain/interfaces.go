// Package chain defines the interfaces that each supported blockchain must implement.
// Adding a new chain requires implementing BlockchainClient, BlockParser, and
// AddressGenerator, then registering them via NewChainConfig.
package chain

import (
	"time"

	"github.com/btcsuite/btcd/chaincfg"
)

// BlockchainClient handles communication with a chain's block explorer / node API.
type BlockchainClient interface {
	// GetTipHeight returns the current chain tip height.
	GetTipHeight() (uint64, error)
	// GetBlockHashAtHeight returns the block hash at the given height.
	// Returns (hash, httpStatus, error) — httpStatus is 404 when the height doesn't exist yet.
	GetBlockHashAtHeight(height uint64) (string, int, error)
	// GetRawBlock returns the raw block bytes for the given hash.
	GetRawBlock(hash string) ([]byte, error)
	// GetAddressTxs returns the transaction history for an address.
	GetAddressTxs(address string) ([]TxHistoryEntry, error)
	// GetTxStatus checks whether a transaction is confirmed.
	GetTxStatus(txid string) (bool, error)
	// GetTxDetails returns confirmation details for a transaction.
	// Returns a zero-value TxConfirmationDetails and no error if the tx is not yet confirmed.
	GetTxDetails(txid string) (TxConfirmationDetails, error)
	// PostTx broadcasts a raw signed transaction.
	PostTx(rawTx string) error
}

// TxConfirmationDetails holds the on-chain position of a confirmed transaction.
type TxConfirmationDetails struct {
	Confirmed   bool
	BlockHeight uint64
	BlockHash   string
	// TxIndex is the position of the transaction within the block (0-based).
	TxIndex uint32
	// IsInstantLocked is true when the chain natively supports InstantSend
	// (currently only Dash) and dashd has accepted an LLMQ IS-lock for this
	// tx. Independent of Confirmed: an IS-locked tx can be either in the
	// mempool (Confirmed=false) or already in a block (Confirmed=true).
	// Other chains always leave this false.
	IsInstantLocked bool
}

// TxHistoryEntry is a chain-agnostic representation of a historical transaction
// for address scanning.
type TxHistoryEntry struct {
	TxID      string
	Confirmed bool
	Outputs   []TxOutput
}

// TxOutput is a single output from a transaction.
type TxOutput struct {
	Address string
	Value   int64
	Index   uint32
}

// MappingInput is the data extracted from a block that the mapping contract needs.
type MappingInput struct {
	RawTxHex       string
	MerkleProofHex string
	TxIndex        uint32
	BlockHeight    uint32
}

// BlockParser handles chain-specific block deserialization and transaction extraction.
type BlockParser interface {
	// ParseBlock extracts mapping-relevant data from raw block bytes.
	// Returns matched addresses → MappingInput data.
	ParseBlock(rawBlock []byte, knownAddresses []string, blockHeight uint64) ([]MappingInput, error)
}

// AddressGenerator creates deposit addresses for the chain.
type AddressGenerator interface {
	// GenerateDepositAddress creates a deposit address from public keys and an instruction tag.
	// Returns (address, witnessScript, error).
	GenerateDepositAddress(primaryPubKeyHex, backupPubKeyHex, instruction string) (string, []byte, error)
}

// MempoolWatcher is an optional capability for chains whose client can be told
// to track addresses for mempool deposits (via a ZMQ feed or similar). Chains
// that don't support it return nil from `Chain.Client.(MempoolWatcher)`. The
// mapping bot type-asserts on this interface — having it not implemented just
// degrades the bot to confirmed-only deposit detection, no errors.
type MempoolWatcher interface {
	// WatchAddress registers an address whose mempool deposits should be
	// tracked. Idempotent.
	WatchAddress(addr string)
	// UnwatchAddress stops tracking an address.
	UnwatchAddress(addr string)
}

// DepositEvent is fired when a real-time chain feed sees a transaction whose
// output pays a watched deposit address. Currently only emitted by the dashd
// client (via its ZMQ subscriber); other clients leave the channel nil.
type DepositEvent struct {
	// TxID is the canonical txid (reversed-hex form) of the transaction.
	TxID string
	// RawTxHex is the serialized transaction encoded as hex, ready to put
	// straight into a mapInstantSend payload.
	RawTxHex string
	// Address is the deposit address that was matched.
	Address string
	// InstantLocked is true when the event came from a ZMQ rawtxlock topic
	// (the network has accepted an LLMQ IS-lock for this tx). False on plain
	// rawtx events.
	InstantLocked bool
}

// DepositNotifier is an optional capability for chains that emit real-time
// deposit events. The mapping bot type-asserts on this interface and, when
// available, runs a goroutine that consumes events and submits IS-locked txs
// via the contract's mapInstantSend action.
type DepositNotifier interface {
	// Deposits returns a channel of deposit events. Implementations should
	// return the same channel on every call. Closed when the underlying
	// client is shut down.
	Deposits() <-chan DepositEvent
}

// ChainConfig bundles all chain-specific components together.
type ChainConfig struct {
	// Name is the chain identifier (e.g., "btc", "ltc", "dash").
	Name string
	// AssetSymbol is the token symbol used in contract calls (e.g., "BTC", "LTC", "DASH").
	AssetSymbol string
	// Client is the blockchain API client.
	Client BlockchainClient
	// Parser handles block parsing and address/tx extraction.
	Parser BlockParser
	// AddressGen creates chain-specific deposit addresses.
	AddressGen AddressGenerator
	// BlockInterval is the expected time between blocks.
	BlockInterval time.Duration
	// SleepInterval is how long to sleep when at the chain tip before checking for a new block.
	// Shorter than BlockInterval since some chains have variable block times.
	SleepInterval time.Duration
	// DropHeightDiff is how many blocks old an address must be before cleanup.
	DropHeightDiff uint64
	// HistoricalTxLookback caps how far back (in blocks) HandleExistingTxs
	// scans when a new address is registered. Sized to roughly one week of
	// chain time — longer than a typical operational restart gap, shorter
	// than the mapping cleanup horizon.
	HistoricalTxLookback uint64
	// ChainParams holds btcsuite-compatible chain parameters (for UTXO chains).
	// Nil for non-UTXO chains (e.g., ETH).
	ChainParams *chaincfg.Params
	// DefaultDbName is the default MongoDB database name for this chain.
	DefaultDbName string
}
