package kafkatx

import "github.com/twmb/franz-go/pkg/kgo"

// TracksRecordsForTest reports whether the client carries a record ledger
// this package finds, so a test can check that the Kafka driver puts one on a
// transactional producer.
func TracksRecordsForTest(cl *kgo.Client) bool { return ledgerOf(cl) != nil }
