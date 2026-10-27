package pulsar_test

import (
	"context"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
)

// clientProperties are the property names pulsar-client-go writes and reads on
// its own account, which a caller's header must never be published as.
var clientProperties = [...]string{
	pulsar.SysPropertyDelayTime,
	pulsar.SysPropertyRealTopic,
	pulsar.SysPropertyRetryTopic,
	pulsar.SysPropertyReconsumeTimes,
	pulsar.SysPropertyOriginMessageID,
	pulsar.PropertyOriginMessageID,
}

// TestAHeaderNamedLikeAClientPropertyIsRefusedInTheStrictMode.
//
// The client and the driver act on these names, and the decoder strips them,
// so the strict mode has no way to carry one as itself.
func TestAHeaderNamedLikeAClientPropertyIsRefusedInTheStrictMode(t *testing.T) {
	b, _ := newRig(t, nil)
	snk := sinkOf(t, b, "sysprops", mq.MetadataNativeStrict)
	for _, k := range clientProperties {
		m := &mq.Message{Body: []byte("x")}
		m.Headers.Set(k, []byte("1"))
		err := snk.Publish(context.Background(), m, nil)
		if mq.CodeOf(err) != mq.CodeUnsupported {
			t.Errorf("publishing a header named %q is %v, want an Unsupported refusal: pulsar-client-go "+
				"owns that property", k, err)
		}
	}
}

// TestAReceivedHeaderCannotSpoofTheAttemptOrTheTopic.
//
// A caller's RECONSUMETIMES once went out as a native property, and the
// receiving driver added it to the delivery attempt; a caller's REAL_TOPIC
// renamed the topic the delivery reported. The adaptive mode now envelopes such
// a header and the lenient mode drops it, so neither reaches the properties the
// driver reads.
func TestAReceivedHeaderCannotSpoofTheAttemptOrTheTopic(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode mq.MetadataMode
		kept bool
	}{
		{"adaptive", mq.MetadataAdaptive, true},
		{"lenient", mq.MetadataLenientDrop, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := newRig(t, nil)
			src := sourceOf(t, b, "spoofed", "workers", tc.mode, 4)
			snk := sinkOf(t, b, "spoofed", tc.mode)
			m := &mq.Message{Body: []byte("x")}
			m.Headers.Set(pulsar.SysPropertyReconsumeTimes, []byte("41"))
			m.Headers.Set(pulsar.SysPropertyRealTopic, []byte("persistent://public/default/elsewhere"))
			publish(t, snk, m)

			d := fetchOne(t, src)
			if d.Info.Attempt != 1 {
				t.Errorf("the first delivery reports attempt %d, want 1: a caller's header was read as the "+
					"client's retry count", d.Info.Attempt)
			}
			if d.Info.Topic != "spoofed" {
				t.Errorf("the delivery reports topic %q, want %q: a caller's header was read as the "+
					"client's real topic", d.Info.Topic, "spoofed")
			}
			v, ok := d.Message.Headers.Get(pulsar.SysPropertyReconsumeTimes)
			if tc.kept && (!ok || string(v) != "41") {
				t.Errorf("the header came back as %q (present %v), want it carried as the caller set it", v, ok)
			}
			if !tc.kept && ok {
				t.Errorf("the header came back as %q in the lenient mode, which drops what it cannot carry", v)
			}
		})
	}
}
