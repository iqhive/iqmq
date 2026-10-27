package gcppubsub

import (
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestLenientDropRefusesAnOversizedNonHeaderValue holds MetadataLenientDrop to
// what the service takes. The mode never envelopes and drops only headers, so
// a key or a typed property longer than the 1024-byte attribute-value limit --
// both inside the envelope's own bounds -- used to reach the attributes as it
// was, and the service refuses the whole publication for it. It is refused
// before any I/O instead, while MetadataAdaptive still carries it in the
// envelope.
func TestLenientDropRefusesAnOversizedNonHeaderValue(t *testing.T) {
	long := strings.Repeat("v", MaxAttributeValueBytes+1)
	for _, tc := range []struct {
		name string
		m    *mq.Message
	}{
		{"key", &mq.Message{Key: []byte(long), Body: []byte("b")}},
		{"content type", &mq.Message{Body: []byte("b"), Properties: mq.Properties{ContentType: long}}},
		{"correlation id", &mq.Message{Body: []byte("b"), Properties: mq.Properties{CorrelationID: long}}},
		{"reply to", &mq.Message{Body: []byte("b"), Properties: mq.Properties{ReplyTo: long}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := mustEncoder(t, Config{ProjectID: "p"}, mq.MetadataLenientDrop).encode(tc.m, &mq.PublishOptions{})
			if err == nil {
				t.Fatalf("MetadataLenientDrop accepted a %d-byte %s, publishing attributes the service refuses: %v", len(long), tc.name, attributeSizes(out))
			}
			if code := mq.CodeOf(err); code != mq.CodeUnsupported {
				t.Fatalf("the refusal reported code %v, want %v: %v", code, mq.CodeUnsupported, err)
			}
			if _, err := mustEncoder(t, Config{ProjectID: "p"}, mq.MetadataAdaptive).encode(tc.m, &mq.PublishOptions{}); err != nil {
				t.Fatalf("MetadataAdaptive refused what the envelope carries: %v", err)
			}
		})
	}
}

// attributeSizes is each attribute's value length, for a failure message.
func attributeSizes(out encoded) map[string]int {
	sizes := make(map[string]int, len(out.Out.Attributes))
	for k, v := range out.Out.Attributes {
		sizes[k] = len(v)
	}
	return sizes
}
