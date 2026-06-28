package snssqs_test

import (
	"context"
	"errors"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	sns "bitbucket.org/iqhive/iqmq/drivers/snssqs"
	"bitbucket.org/iqhive/iqmq/drivers/snssqs/internal/snsfake"
	"bitbucket.org/iqhive/iqmq/drivers/sqs/sqsfake"
)

// The entry points' refusals and the Admin surface's error paths.
//
// Every test in this package reaches for BackendWithAPI against two fakes
// that say yes, so the checks that come before anything is built -- a nil
// client, an ownership that is neither, a configuration that does not
// validate -- ran nowhere, and neither did what each Admin call does when
// the endpoint fails.

// TestOpenWithNoClientIsRefused.
//
// This driver takes TWO clients and needs both, so the refusal names which
// one is missing: without the checks it is a nil dereference on first use,
// and a caller who passed one of the two would be told nothing about which.
func TestOpenWithNoClientIsRefused(t *testing.T) {
	t.Parallel()
	cfg := sns.Config{Region: "fake-region-1", AccessKeyID: "AKIAFAKE", SecretAccessKey: "x"}

	if _, err := sns.BackendWithClients(t.Context(), nil, nil, driverkit.ClientBorrowed, cfg); err == nil {
		t.Error("BackendWithClients accepted two nil clients")
	}
	if _, err := sns.OpenWithClients(t.Context(), nil, nil, driverkit.ClientBorrowed, cfg); err == nil {
		t.Error("OpenWithClients accepted two nil clients")
	}
	if _, err := sns.BackendWithAPI(t.Context(), nil, nil, nil, nil, driverkit.ClientBorrowed, cfg); err == nil {
		t.Error("BackendWithAPI accepted a nil API")
	}
}

// TestEveryEntryPointRespectsACancelledContext.
//
// The check is first, before the configuration is read, so a caller whose
// context is already dead gets that answer rather than a validation error
// about a field they may have set correctly.
func TestEveryEntryPointRespectsACancelledContext(t *testing.T) {
	t.Parallel()
	queues := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	server := snsfake.New(snsfake.Options{Region: "fake-region-1", AccountID: "000000000000", Queues: queues})
	cfg := sns.Config{Region: "fake-region-1", AccessKeyID: "AKIAFAKE", SecretAccessKey: "x"}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := sns.BackendWithAPI(ctx, server, queues, queues, server,
		driverkit.ClientBorrowed, cfg); !errors.Is(err, context.Canceled) {
		t.Errorf("BackendWithAPI with a cancelled context is %v, want context.Canceled", err)
	}
	if _, err := sns.OpenBackend(ctx, cfg); err == nil {
		t.Error("OpenBackend with a cancelled context returned a backend")
	}
	if _, err := sns.Open(ctx, cfg); err == nil {
		t.Error("Open with a cancelled context returned a client")
	}
}

// TestEveryAdminCallReportsAFailingEndpoint.
//
// Each has its own mapping, and a call that swallowed the failure would
// report success for topology that was never created -- the worst outcome
// for a provisioning step, because the next thing to run is whatever assumed
// it worked.
//
// The fault is installed on the operation each call actually MAKES, and the
// EnsureTopic case names a topic this backend has not seen: the driver
// caches a topic's ARN, so faulting CreateTopic after the topic is
// provisioned fails nothing and the test would pass against a driver that
// ignored every error.
func TestEveryAdminCallReportsAFailingEndpoint(t *testing.T) {
	t.Parallel()
	boom := errors.New("the endpoint is unavailable")

	for _, tc := range []struct {
		name string
		op   string
		call func(mq.Admin, string) error
	}{
		{"EnsureTopic", "CreateTopic", func(a mq.Admin, _ string) error {
			// A topic this backend has NOT already resolved: the driver
			// caches a topic's ARN, so re-ensuring the provisioned one
			// short-circuits and the fault would never be consulted.
			return a.EnsureTopic(t.Context(), mq.TopicSpec{Name: "never-ensured-before"})
		}},
		{"DeleteTopic", "DeleteTopic", func(a mq.Admin, n string) error {
			return a.DeleteTopic(t.Context(), n)
		}},
		{"EnsureSubscription", "Subscribe", func(a mq.Admin, n string) error {
			return a.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: n, Name: "workers"})
		}},
		{"ListTopics", "ListTopics", func(a mq.Admin, _ string) error {
			for _, err := range a.ListTopics(t.Context()) {
				if err != nil {
					return err
				}
			}
			return nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			queues := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
			server := snsfake.New(snsfake.Options{
				Region: "fake-region-1", AccountID: "000000000000", Queues: queues,
			})
			h := openOn(t, server, queues)
			a, err := h.back.Admin()
			if err != nil {
				t.Fatalf("Admin(): %v", err)
			}
			name := topic(t, "admin")
			if err := a.EnsureTopic(t.Context(), mq.TopicSpec{Name: name}); err != nil {
				t.Fatalf("provisioning: %v", err)
			}

			server.Fault(snsfake.Fault{Op: tc.op, Err: boom})
			if err := tc.call(a, name); err == nil {
				t.Errorf("%s returned nil while %s was failing", tc.name, tc.op)
			}
		})
	}
}

// TestAdminIsRefusedOnAClosedBackend.
func TestAdminIsRefusedOnAClosedBackend(t *testing.T) {
	t.Parallel()
	h := open(t)
	if err := h.back.Close(); err != nil {
		t.Fatalf("closing the backend: %v", err)
	}
	if _, err := h.back.Admin(); err == nil {
		t.Error("Admin() on a closed backend returned a surface")
	}
}
