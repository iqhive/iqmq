package kafkago

import "testing"

// TestSettlementRefusalTexts pins the texts a settlement returns when its
// consumer group generation has ended and when the nack strategy has stopped
// the consumer. Both used to end with a parenthetical naming a document this
// repository does not hold, and the lease-lost one named the memory engine to
// a Kafka user.
func TestSettlementRefusalTexts(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{
			name: "errLeaseLost",
			err:  errLeaseLost,
			want: "iqmq/kafkago: the consumer group generation ended, so this settlement names a delivery another member now owns",
		},
		{
			name: "errStopped",
			err:  errStopped,
			want: "iqmq/kafkago: the nack strategy stopped this consumer; nothing after the refused record is consumed until it is reopened",
		},
	} {
		if got := tc.err.Error(); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.name, got, tc.want)
		}
	}
}
