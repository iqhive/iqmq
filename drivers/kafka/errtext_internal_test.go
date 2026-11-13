package kafka

import "testing"

// TestSettlementRefusalTexts pins the texts a settlement returns when its
// partition has moved to another member and when the nack strategy has
// stopped the consumer. Both used to end with a parenthetical naming a
// document this repository does not hold, and the lease-lost one named the
// memory engine to a Kafka user.
func TestSettlementRefusalTexts(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{
			name: "errLeaseLost",
			err:  errLeaseLost,
			want: "iqmq/kafka: the partition was revoked or lost, so this settlement names a delivery another member now owns",
		},
		{
			name: "errStopped",
			err:  errStopped,
			want: "iqmq/kafka: the nack strategy stopped this consumer; nothing after the refused record is consumed until it is reopened",
		},
	} {
		if got := tc.err.Error(); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.name, got, tc.want)
		}
	}
}
