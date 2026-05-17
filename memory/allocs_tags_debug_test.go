//go:build iqmq_debug

package memory_test

// allocsDebugCanary reports that the iqmq_debug canaries are compiled in.
//
// Under the tag the engine arms the release contract of every publication —
// Engine.ownership is true whether or not Config.DebugOwnership asked for it
// — and the ledger that detects a second release of one payload costs an
// allocation per record.
const allocsDebugCanary = true
