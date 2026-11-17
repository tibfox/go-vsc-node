package main

import (
	"flag"
	"fmt"
	"os"
)

type args struct {
	network       string
	wasmPath      string
	name          string
	description   string
	owner         string
	isInit        bool
	gqlUrl        string
	dataDir       string
	sysconfigPath string

	// update contract args
	contractId string

	// cancel a pending (timelocked) contract update
	cancelUpdate bool
	cancelTxId   string

	// offline-signing args (Ledger / hardware-wallet flow)
	noBroadcast     bool
	out             string
	expiration      int
	broadcastSigned string
	signature       string

	// one-shot Ledger signing: prepare -> sign via external command -> broadcast
	ledgerSignCmd string
	ledgerPath    string
}

func ParseArgs() (args, error) {
	flag.Usage = func() {
		fmt.Printf("Deploy or update a WASM contract on Magi.\n\n")
		fmt.Printf("Usage: %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}
	network := flag.String("network", "mainnet", "Network to deploy contract to")
	wasmPath := flag.String("wasmPath", "", "Path to compiled WASM bytecode")
	name := flag.String("name", "", "Name of the contract")
	desc := flag.String("description", "", "Description of the contract")
	owner := flag.String("owner", "", "Owner of the contract (defaults to contract deployer)")
	isInit := flag.Bool("init", false, "Generate credentials config files")
	gqlUrl := flag.String(
		"gqlUrl",
		"",
		"GraphQL API URL of a Magi node, used to fetch the latest election (the "+
			"committee's BLS member keys) before requesting a storage proof. Required "+
			"when -wasmPath is given. Mainnet examples: "+
			"https://vsc.techcoderx.com/api/v1/graphql, "+
			"https://magi.milohpr.com/api/v1/graphql. Testnet example: "+
			"https://magi-test.techcoderx.com/api/v1/graphql",
	)
	dataDir := flag.String("data-dir", "data", "Data directory for config")
	contractId := flag.String(
		"contractId",
		"",
		"Existing contract ID to update contract. Omit to deploy a new contract.",
	)
	sysconfigPath := flag.String("sysconfig", "", "Path to JSON file with system config overrides")
	cancelUpdate := flag.Bool(
		"cancel-update",
		false,
		"Cancel a pending (timelocked) contract update for -contractId instead of deploying/updating. Owner-only.",
	)
	cancelTxId := flag.String(
		"cancel-tx",
		"",
		"With -cancel-update: tx id of the specific queued update to cancel. Omit to cancel all pending updates for the contract.",
	)
	noBroadcast := flag.Bool(
		"no-broadcast",
		false,
		"Build and print the unsigned transaction + signing digest instead of broadcasting. Use to sign externally (e.g. a Ledger), then submit with -broadcast-signed.",
	)
	out := flag.String("out", "", "When used with -no-broadcast, also write the signing bundle JSON to this file path.")
	expiration := flag.Int(
		"expiration",
		1800,
		"Seconds until the prepared transaction expires (max 3600). Larger values give more time to confirm on a hardware wallet. Only used with -no-broadcast.",
	)
	broadcastSigned := flag.String(
		"broadcast-signed",
		"",
		"Path to a signing bundle previously produced by -no-broadcast. Broadcasts it using the signature from -signature. Requires no private key and no WASM proof.",
	)
	signature := flag.String(
		"signature",
		"",
		"Hex-encoded signature (from the external signer) to attach when broadcasting with -broadcast-signed.",
	)
	ledgerSignCmd := flag.String(
		"ledger-sign-cmd",
		"",
		"External command that signs the active authority (e.g. the bundled ledger-signer after 'pnpm install': \"node cmd/contract-deployer/ledger-signer/dist/sign.js\"). Run via 'sh -c'; receives the signing request JSON on stdin and must print the signature hex on stdout. When set, the tool prepares, signs, and broadcasts in one run.",
	)
	ledgerPath := flag.String(
		"ledger-path",
		"m/48'/13'/1'/0'/0'",
		"BIP32 (SLIP-0048) derivation path passed to the external signer. Only used with -ledger-sign-cmd.",
	)
	flag.Parse()

	parsed := args{
		network:         *network,
		wasmPath:        *wasmPath,
		name:            *name,
		description:     *desc,
		owner:           *owner,
		isInit:          *isInit,
		gqlUrl:          *gqlUrl,
		dataDir:         *dataDir,
		sysconfigPath:   *sysconfigPath,
		contractId:      *contractId,
		cancelUpdate:    *cancelUpdate,
		cancelTxId:      *cancelTxId,
		noBroadcast:     *noBroadcast,
		out:             *out,
		expiration:      *expiration,
		broadcastSigned: *broadcastSigned,
		signature:       *signature,
		ledgerSignCmd:   *ledgerSignCmd,
		ledgerPath:      *ledgerPath,
	}

	if parsed.expiration <= 0 || parsed.expiration > 3600 {
		return parsed, fmt.Errorf(
			"-expiration must be between 1 and 3600 seconds (Hive's maximum), got %d",
			parsed.expiration,
		)
	}
	if parsed.broadcastSigned != "" {
		if parsed.noBroadcast {
			return parsed, fmt.Errorf("-broadcast-signed cannot be combined with -no-broadcast")
		}
		if parsed.signature == "" {
			return parsed, fmt.Errorf("-broadcast-signed requires -signature")
		}
		if parsed.ledgerSignCmd != "" {
			return parsed, fmt.Errorf("-broadcast-signed cannot be combined with -ledger-sign-cmd")
		}
	}
	if parsed.ledgerSignCmd != "" && parsed.noBroadcast {
		return parsed, fmt.Errorf("-ledger-sign-cmd cannot be combined with -no-broadcast")
	}

	// -gqlUrl has no default on purpose. It used to default to
	// https://api.vsc.eco/api/v1/graphql, dead since 2026-09-17 (dial tcp
	// 149.56.25.168:443: i/o timeout) — the same dead host B17 removed as the
	// evm-mapping-bot's GRAPHQL_URLS default. Because the only consumer here is
	// client.RequestProof -> FetchElection, every deploy that relied on the
	// default failed late, at the storage-proof step, with a confusing network
	// error after the whole P2P stack had already spun up. Demanding the flag
	// fails immediately instead, with a message naming live endpoints.
	//
	// It is only needed when a storage proof is actually requested: -init,
	// -cancel-update and -broadcast-signed all return before that, so they must
	// keep working without it.
	if parsed.wasmPath != "" && parsed.broadcastSigned == "" && !parsed.cancelUpdate &&
		parsed.gqlUrl == "" {
		return parsed, fmt.Errorf(
			"-gqlUrl is required when deploying or updating a contract with -wasmPath: " +
				"it is how the deployer fetches the current election to build the storage " +
				"proof. Pass a live Magi node, e.g. " +
				"-gqlUrl https://vsc.techcoderx.com/api/v1/graphql (mainnet) or " +
				"-gqlUrl https://magi-test.techcoderx.com/api/v1/graphql (testnet)",
		)
	}

	return parsed, nil
}
