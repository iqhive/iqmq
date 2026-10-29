package kafkatx

import "github.com/twmb/franz-go/pkg/kgo"

// TracksRecordsForTest reports whether the client carries a record ledger,
// so a test that needs the Kafka driver's own can tell a driver release that
// predates it from one that puts it on a transactional producer.
func TracksRecordsForTest(cl *kgo.Client) bool { return ledgerOf(cl) != nil }
