package transactionpool

import "os"

// rcBypassEnabled — review6 E (devnet-harness fix): when the env var
// VSC_TEST_BYPASS_RC=1 is set, the ingestion gate skips the per-account
// RC pre-flight and the StaticMaxRcCost cap. This is for in-process e2e
// test harnesses ONLY — production builds never set the env var; the
// gate is checked at the call site so its cost is one os.Getenv per
// IngestTx call, negligible.
//
// Rationale: the e2e/modules/e2e harness boots a single test process
// with multiple in-memory nodes. Test accounts don't accumulate Hive
// balance through real L1 deposits, so they read as ~500 RC (the
// no-balance default), which trips the RcLimit pre-flight at any
// contract-call rc_limit > 500. The bypass restores test-side liveness
// without weakening any production gate.
//
// IMPORTANT: keep this isolated in its own file so a future production
// audit grep for "RC bypass" hits this single point. Never call from
// production code paths; never default-true.
func rcBypassEnabled() bool {
	return os.Getenv("VSC_TEST_BYPASS_RC") == "1"
}
