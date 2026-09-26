package sqs_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	sq "github.com/iqhive/iqmq/drivers/sqs"
)

// TestCapabilitiesConsidersEveryFeature is what keeps the table honest as the inventory grows: an unadvertised feature renders as unsupported, which
// is the safe answer and is indistinguishable from one the driver forgot.
//
// The construction itself is the assertion: capabilities() refuses a table
// that does not mention every feature, so a backend that opens has one.
func TestCapabilitiesConsidersEveryFeature(t *testing.T) {
	for _, fifo := range []bool{false, true} {
		h := open(t, func(c *sq.Config) { c.FIFO = fifo })
		caps := h.client.Capabilities()
		for f := range mq.FeatureCount {
			s := caps.Support(mq.Feature(f))
			if !s.Level.Valid() {
				t.Fatalf("fifo=%v: feature %v has level %v", fifo, mq.Feature(f), s.Level)
			}
			switch s.Level {
			case mq.Conditional, mq.EmulatedVolatile, mq.EmulatedDurable:
				if s.Note == "" {
					t.Errorf("fifo=%v: %v is %v with no note; the specification requires the condition or the emulation to be stated", fifo, mq.Feature(f), s.Level)
				}
			case mq.Native, mq.Unsupported:
			}
		}
	}
}

// TestProfilesDifferWhereTheyShould is the reason [Config.FIFO] is reported as
// a profile rather than hidden in a flag.
//
// Each of these claims is FIFO's and is absent from the standard profile, or
// the other way round. A driver that answered the same table for both would
// let a caller plan an ordering guarantee against a queue that has none.
func TestProfilesDifferWhereTheyShould(t *testing.T) {
	std := open(t, func(c *sq.Config) { c.FIFO = false }).client.Capabilities()
	fifo := open(t, func(c *sq.Config) { c.FIFO = true }).client.Capabilities()
	for _, f := range []mq.Feature{
		mq.FeatureOrderedByKey, mq.FeatureDeduplication, mq.FeatureIdempotentPublish,
	} {
		if std.Has(f) {
			t.Errorf("the standard profile claims %v", f)
		}
		if !fifo.Has(f) {
			t.Errorf("the FIFO profile does not claim %v", f)
		}
	}
	// And the other way: a per-message timer is the standard profile's and
	// the FIFO profile has none.
	if !std.Has(mq.FeatureScheduledPublish) {
		t.Error("the standard profile does not claim a scheduled publication, which DelaySeconds is")
	}
	if fifo.Has(mq.FeatureScheduledPublish) {
		t.Error("the FIFO profile claims a scheduled publication; a FIFO queue takes no per-message delay")
	}
}

// TestLimitsAreTheServiceLimits holds the specification's "service limits
// discovered/documented from the tested API version": each number is the
// pinned SDK's, and each unknown is honestly unknown.
func TestLimitsAreTheServiceLimits(t *testing.T) {
	h := open(t)
	lim := h.client.Capabilities().Limits()
	for _, tc := range []struct {
		name  string
		known bool
		got   int64
		want  int64
	}{
		{"MaxMessageBytes", lim.MaxMessageBytes.Known, lim.MaxMessageBytes.Value, sq.MaxMessageBytes},
		{"MaxBatchMessages", lim.MaxBatchMessages.Known, int64(lim.MaxBatchMessages.Value), sq.MaxBatchEntries},
		{"MaxBatchBytes", lim.MaxBatchBytes.Known, lim.MaxBatchBytes.Value, sq.MaxMessageBytes},
		{"MaxHeaderCount", lim.MaxHeaderCount.Known, int64(lim.MaxHeaderCount.Value), sq.MaxAttributes - 1},
		{"MaxDelay", lim.MaxDelay.Known, int64(lim.MaxDelay.Value), int64(sq.MaxDelay)},
		{"MaxLease", lim.MaxLease.Known, int64(lim.MaxLease.Value), int64(sq.MaxVisibility)},
	} {
		if !tc.known {
			t.Errorf("%s is unknown; the pinned SDK states it", tc.name)
			continue
		}
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.want)
		}
	}
	// MaxHeaderBytes is deliberately unknown: the service bounds the whole
	// message and states no separate header ceiling, and the specification forbids
	// reading an unadvertised limit as zero.
	if lim.MaxHeaderBytes.Known {
		t.Errorf("MaxHeaderBytes is advertised as %d; the service states no header ceiling of its own", lim.MaxHeaderBytes.Value)
	}
	// The FIFO profile has no per-message timer, so it advertises no delay
	// ceiling rather than a zero one.
	fifo := open(t, func(c *sq.Config) { c.FIFO = true }).client.Capabilities().Limits()
	if fifo.MaxDelay.Known {
		t.Errorf("the FIFO profile advertises a delay ceiling of %v; it takes no per-message delay at all", fifo.MaxDelay.Value)
	}
}

// TestMaxMessageBytesFollowsTheConfiguration is what makes the
// oversized-payload conformance scenario cheap and real: a queue configured
// with a smaller ceiling advertises the smaller one.
func TestMaxMessageBytesFollowsTheConfiguration(t *testing.T) {
	h := open(t, func(c *sq.Config) { c.MaxMessageBytes = 4096 })
	if got := h.client.Capabilities().Limits().MaxMessageBytes; !got.Known || got.Value != 4096 {
		t.Fatalf("MaxMessageBytes = %+v, want the configured 4096", got)
	}
}

// TestInfoNamesTheClient is an empty string is "the driver did not say",
// so the fields that ARE known have to be filled.
func TestInfoNamesTheClient(t *testing.T) {
	h := open(t)
	info := h.client.Info()
	if info.Name != "sqs" {
		t.Errorf("Name = %q", info.Name)
	}
	if info.Profile != "standard" {
		t.Errorf("Profile = %q", info.Profile)
	}
	if info.ClientName == "" || info.ClientVersion == "" {
		t.Errorf("the client library is %q %q", info.ClientName, info.ClientVersion)
	}
	if h.client.Native() == nil {
		t.Error("Native is nil, so the escape hatch is not offered")
	}
}

// TestHealthReachesDegradedAndBack is for this driver: a poller that
// fails has nowhere to surface but Health, so Health has to move.
func TestHealthReachesDegradedAndBack(t *testing.T) {
	h := open(t)
	if got := h.client.Health().State; got != mq.HealthReady {
		t.Fatalf("a freshly opened backend reports %v", got)
	}
	name := topic(t, "health")
	h.publish(name, &mq.Message{Body: []byte("b")})
	// Every receive fails from here on, which is what a deleted queue or an
	// expired credential looks like.
	h.server.Fault(fault("ReceiveMessage", h.queue(name)))
	cons := h.consumer(name, 1)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if h.client.Health().State == mq.HealthDegraded {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	snap := h.client.Health()
	if snap.State != mq.HealthDegraded {
		t.Fatalf("the backend reports %v after every poll failed; the specification asks a driver's background failures to surface", snap.State)
	}
	if snap.Err == nil {
		t.Fatal("the degraded state carries no failure, so nothing says what went wrong")
	}
	// The message arrives once the faults stop, and the state clears.
	_ = cons
	d := h.receiveOne(cons, 10*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if got := h.client.Health().State; got != mq.HealthReady {
		t.Fatalf("the backend still reports %v after a successful poll", got)
	}
}
