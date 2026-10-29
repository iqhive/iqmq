package activemq

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/internal/activemq"
)

// What Admin destroys on Artemis, against a broker that holds other
// applications' queues as well as this driver's.
//
// A management round trip needs a broker, so these tests stand in for one
// with [scriptedManagement]: it answers the operations Admin sends the way
// Artemis does, from a table of queues and the address each is bound to, and
// it records what was destroyed.

// scriptedManagement is an Artemis broker's management address, reduced to
// the operations the delete path sends.
type scriptedManagement struct {
	// queues maps every queue on the broker to its address.
	queues map[string]string
	// destroyed and deleted are the queues and addresses removed, in order.
	destroyed []string
	deleted   []string
}

// roundTrip answers one management operation.
func (m *scriptedManagement) roundTrip(_ context.Context, resource, op string, args ...any) (string, error) {
	switch {
	case resource == "broker" && op == "getQueueNames":
		body, err := json.Marshal([][]string{slices.Sorted(maps.Keys(m.queues))})
		return string(body), err
	case strings.HasPrefix(resource, "queue.") && op == "getAddress":
		addr, ok := m.queues[strings.TrimPrefix(resource, "queue.")]
		if !ok {
			return "", &ManagementError{Reason: "AMQ229067: Cannot find resource with name " + resource}
		}
		body, err := json.Marshal([]string{addr})
		return string(body), err
	case resource == "broker" && op == "destroyQueue":
		q, _ := args[0].(string)
		if _, ok := m.queues[q]; !ok {
			return "", &ManagementError{Reason: "AMQ229017: Queue " + q + " does not exist"}
		}
		delete(m.queues, q)
		m.destroyed = append(m.destroyed, q)
		return "[]", nil
	case resource == "broker" && op == "deleteAddress":
		addr, _ := args[0].(string)
		for q, on := range m.queues {
			if on == addr {
				// What Artemis answers while a queue is still bound.
				return "", &ManagementError{Reason: "AMQ229205: Address " + addr + " has bindings: " + q}
			}
		}
		m.deleted = append(m.deleted, addr)
		return "[]", nil
	}
	return "", errors.New("the scripted broker has no answer for " + op + " on " + resource)
}

// adminOver is this driver's Artemis Admin talking to m.
func adminOver(cfg Config, m *scriptedManagement) *artemisAdmin {
	return &artemisAdmin{cfg: cfg, roundTrip: m.roundTrip}
}

// TestDeleteTopicDestroysOnlyTheQueuesOnTheTopicsAddress is DeleteTopic with
// no Prefix, on a broker where other applications' queues have names that
// read like the topic's subscriptions.
//
// Every queue whose name parsed as `orders.<something>` was destroyed as one
// of the topic's, so a dead-letter queue `orders.dlq` and a JMS durable
// subscription `orders.audit`, both on other addresses, went with their
// messages.
func TestDeleteTopicDestroysOnlyTheQueuesOnTheTopicsAddress(t *testing.T) {
	m := &scriptedManagement{queues: map[string]string{
		"orders.workers": "orders",
		"orders.billing": "orders",
		"orders.dlq":     "DLA",
		"orders.audit":   "payments",
		"payments.audit": "payments",
		"DLQ":            "DLQ",
	}}
	a := adminOver(Config{Broker: activemq.Artemis}, m)
	if err := a.DeleteTopic(context.Background(), "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	slices.Sort(m.destroyed)
	if want := []string{"orders.billing", "orders.workers"}; !slices.Equal(m.destroyed, want) {
		t.Errorf("DeleteTopic(orders) destroyed %v, want %v: a queue bound to another address is another "+
			"application's, whatever its name says", m.destroyed, want)
	}
	if want := []string{"orders"}; !slices.Equal(m.deleted, want) {
		t.Errorf("DeleteTopic(orders) deleted the addresses %v, want %v", m.deleted, want)
	}
	for _, q := range []string{"orders.dlq", "orders.audit", "payments.audit", "DLQ"} {
		if _, ok := m.queues[q]; !ok {
			t.Errorf("DeleteTopic(orders) destroyed %s, which is bound to another address", q)
		}
	}
}

// TestDeleteSubscriptionLeavesAQueueOnAnotherAddressAlone is the same check
// for one subscription: the queue its name derives to is bound to another
// address, so the subscription does not exist and there is nothing to delete.
// A missing queue is nothing to delete as well.
func TestDeleteSubscriptionLeavesAQueueOnAnotherAddressAlone(t *testing.T) {
	m := &scriptedManagement{queues: map[string]string{"orders.dlq": "DLA"}}
	a := adminOver(Config{Broker: activemq.Artemis}, m)
	for _, sub := range []string{"dlq", "gone"} {
		if err := a.DeleteSubscription(context.Background(), "orders", sub); err != nil {
			t.Errorf("DeleteSubscription(orders, %s): %v, want nil: there is no such subscription", sub, err)
		}
	}
	if len(m.destroyed) != 0 {
		t.Errorf("DeleteSubscription destroyed %v, a queue bound to another address", m.destroyed)
	}
}

// TestDeleteTopicUnderQueueAddressingChecksTheQueuesAddress is the JMS-queue
// mapping, where the topic is a queue of the same name on its own address. A
// queue of that name on another address is not the topic's.
func TestDeleteTopicUnderQueueAddressingChecksTheQueuesAddress(t *testing.T) {
	m := &scriptedManagement{queues: map[string]string{"orders": "legacy"}}
	a := adminOver(Config{Broker: activemq.Artemis, Addressing: AddressQueue}, m)
	if err := a.DeleteTopic(context.Background(), "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if len(m.destroyed) != 0 {
		t.Errorf("DeleteTopic(orders) destroyed %v, a queue called orders bound to another address", m.destroyed)
	}

	m = &scriptedManagement{queues: map[string]string{"orders": "orders"}}
	a = adminOver(Config{Broker: activemq.Artemis, Addressing: AddressQueue}, m)
	if err := a.DeleteTopic(context.Background(), "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if want := []string{"orders"}; !slices.Equal(m.destroyed, want) || !slices.Equal(m.deleted, want) {
		t.Errorf("DeleteTopic(orders) destroyed %v and deleted %v, want the queue and the address orders",
			m.destroyed, m.deleted)
	}
}
