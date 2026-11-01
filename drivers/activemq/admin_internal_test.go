package activemq

import (
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// Whose a queue is, on Artemis.
//
// An Artemis queue name is unique across the whole broker, and a name this
// driver derives can be held by a queue on another address: a dead-letter
// queue `orders.dlq`, a JMS durable subscription `orders.audit`, or another
// application's JMS queue `orders.billing`. Admin asks the broker which address
// the queue is bound to before it destroys one or reports one as ensured, and
// [ownershipOf] is how it reads the answer. The calls that ask are tested
// against the pinned broker, by the admin-queue-ownership subtest of
// conformance/suites' TestConformance_activemq_artemis.

// readings names each ownership for a failure message.
var readings = map[ownership]string{queueAbsent: "absent", queueOwned: "owned", queueForeign: "foreign"}

// TestOwnershipOfReadsTheBrokersAnswer pins the reading both paths act on.
// A queue is the name set's own only when the broker says it is bound to the
// name set's address. DeleteTopic used to destroy every queue whose name parsed
// as one of the topic's subscriptions, and EnsureSubscription used to report a
// subscription as there whenever a queue of its name existed.
func TestOwnershipOfReadsTheBrokersAnswer(t *testing.T) {
	unavailable := errors.New("iqmq/activemq: connection reset by peer")
	forbidden := &ManagementError{Reason: "AMQ229032: User: reader does not have permission='MANAGE' on address activemq.management"}
	for _, tc := range []struct {
		name    string
		bound   string
		lookup  error
		address string
		want    ownership
		wantErr error
	}{
		{name: "a subscription queue on the topic's address", bound: "orders", address: "orders", want: queueOwned},
		{name: "a dead-letter queue named like a subscription", bound: "DLA", address: "orders", want: queueForeign},
		{name: "a JMS durable subscription on another topic's address", bound: "payments", address: "orders", want: queueForeign},
		{name: "another application's JMS queue on its own address", bound: "orders.billing", address: "orders", want: queueForeign},
		{name: "a subscription queue under a JMS queue's name", bound: "orders", address: "orders.billing", want: queueForeign},
		{name: "a JMS queue on its own address", bound: "orders", address: "orders", want: queueOwned},
		{name: "a JMS queue's name on another address", bound: "legacy", address: "orders", want: queueForeign},
		{name: "an address that differs only in case", bound: "Orders", address: "orders", want: queueForeign},
		{name: "an address that only starts with the topic's", bound: "orders.eu", address: "orders", want: queueForeign},
		{
			name:   "no queue of that name, as getAddress answers it",
			lookup: &ManagementError{Reason: "AMQ229067: Cannot find resource with name queue.orders.gone"},
			want:   queueAbsent, address: "orders",
		},
		{
			name:   "no queue of that name, as destroyQueue answers it",
			lookup: &ManagementError{Reason: "AMQ229017: Queue orders.gone does not exist"},
			want:   queueAbsent, address: "orders",
		},
		{name: "a dead connection", lookup: unavailable, address: "orders", want: queueAbsent, wantErr: unavailable},
		{name: "a refused question", lookup: forbidden, address: "orders", want: queueAbsent, wantErr: forbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ownershipOf(tc.bound, tc.lookup, tc.address)
			if got != tc.want || !errors.Is(err, tc.wantErr) || (err == nil) != (tc.wantErr == nil) {
				t.Errorf("ownershipOf(%q, %v, %q) = %s, %v; want %s, %v",
					tc.bound, tc.lookup, tc.address, readings[got], err, readings[tc.want], tc.wantErr)
			}
		})
	}
}

// TestAnAddressThatAlreadyExistsIsAConflict is Artemis's AMQ229204, "Address
// already exists", through this driver's own error rules. It was mapped to
// CodeNotFound, the opposite of what the broker said: a caller told the address
// was missing would try to create the one the broker had just refused to create
// twice. It is the address's twin of AMQ229019 for a queue, and both are
// conflicts.
func TestAnAddressThatAlreadyExistsIsAConflict(t *testing.T) {
	m := driverkit.ErrorMapper{Driver: DriverName, Rules: errorRules()}
	for _, reason := range []string{
		"AMQ229204: Address already exists: orders",
		"AMQ229019: Queue orders.workers already exists on address orders",
	} {
		err := m.Map(mq.OpAdmin, "orders", "", &ManagementError{Reason: reason})
		if got := mq.CodeOf(err); got != mq.CodeConflict {
			t.Errorf("the management refusal %q maps to %v, want Conflict", reason, got)
		}
	}
}
