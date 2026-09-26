package driverkit_test

import (
	"bytes"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/wire"
)

// adaptiveHeaderless is the envelope of a broker with no headers at all under
// MetadataAdaptive: NSQ's, which is the profile that makes adaptive mode's
// in-band discrimination load-bearing rather than decorative.
func adaptiveHeaderless(t *testing.T) *driverkit.Envelope {
	t.Helper()
	caps := driverkit.NewCapabilities().Native(mq.FeatureAtLeastOnce, mq.ScopeTopic).MustBuild()
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:         mq.MetadataAdaptive,
		Capabilities: caps,
		Limits:       envelopeLimits(),
	})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	return env
}

// envelopeOf is the encoding of m under the default layout: a byte string a
// caller can publish as a NATIVE body, which is the whole point below.
func envelopeOf(t *testing.T, m *mq.Message) []byte {
	t.Helper()
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	out, err := env.Wrap(m)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	return out.Body
}

// TestAdaptiveCarriesANativeBodyThatBeginsLikeAnEnvelope is the recorded defect
// of MetadataAdaptive, in the profile where it is unavoidable.
//
// Unwrap told an enveloped record from a native one with wire.Detect, which
// reads six bytes — "IQMQ", a layout/version byte and a flags byte — so a
// caller that published one of those byte strings as a body got it back decoded
// as metadata. On a broker with somewhere to record the answer out of band the
// sniff is avoidable (drivers/sqs sets an "iqmq-env" message attribute and
// never sniffs at all); on a broker with nothing out of band it is the only
// mechanism there is, so the mechanism has to be right.
//
// Both faces of it are here. A body that only STARTS like an envelope failed to
// decode, and Unwrap returned that failure — so the record could not be
// delivered, so it could not be settled, which is the disqualifier reached
// through a publication the driver accepted. A body that IS a valid envelope
// decoded, and the caller received a different message from the one that was
// published, silently.
func TestAdaptiveCarriesANativeBodyThatBeginsLikeAnEnvelope(t *testing.T) {
	// A message with no metadata at all: adaptive mode has no reason to
	// envelope it, so the body goes to the broker as it stands.
	decodes := envelopeOf(t, &mq.Message{Body: []byte("inner"), ID: "inner-1"})
	for name, body := range map[string][]byte{
		"a prefix that is not a whole envelope": []byte("IQMQ\x11\x00"),
		"a prefix followed by a payload":        []byte("IQMQ\x01\x00 and then some ordinary bytes"),
		"a byte string that IS an envelope":     decodes,
	} {
		t.Run(name, func(t *testing.T) {
			env := adaptiveHeaderless(t)
			m := &mq.Message{Topic: "t", Body: body}
			out, err := env.Wrap(m)
			if err != nil {
				t.Fatalf("Wrap refused a message with no metadata: %v", err)
			}
			var back mq.Message
			if err := env.Unwrap(&back, out); err != nil {
				t.Fatalf("a native body was refused on the way back: %v\n"+
					"the record can be neither decoded nor settled, which is "+
					"the disqualifier reached through a publication the "+
					"driver accepted", err)
			}
			if !bytes.Equal(back.Body, body) {
				t.Fatalf("the body came back as %q, want %q: a native publication was "+
					"decoded as metadata", back.Body, body)
			}
			if back.ID != "" || back.Headers.Len() != 0 {
				t.Fatalf("the delivery carries metadata the publication did not: "+
					"id=%q headers=%d", back.ID, back.Headers.Len())
			}
		})
	}
}

// TestAdaptiveRefusesAnEnvelopeItsLimitsReject is the other side of the fix, and
// the reason the discrimination cannot simply fall back to "native" whenever a
// decode fails.
//
// A producer whose limits are wider than the consumer's writes a real envelope
// the consumer will not decode. That must be a refusal carrying
// CodeMessageTooLarge — the limit exists to produce it — and never a native
// delivery whose body is the envelope's own bytes, which would hand the
// application a payload nobody published.
func TestAdaptiveRefusesAnEnvelopeItsLimitsReject(t *testing.T) {
	wide, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:   mq.MetadataPortableEnvelope,
		Limits: &wire.Limits{MaxHeaderValue: -1},
	})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	m := &mq.Message{Topic: "t", Body: []byte("b")}
	m.Headers.Add("big", bytes.Repeat([]byte("v"), 4096))
	out, err := wide.Wrap(m)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}

	caps := driverkit.NewCapabilities().Native(mq.FeatureAtLeastOnce, mq.ScopeTopic).MustBuild()
	narrow, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:         mq.MetadataAdaptive,
		Capabilities: caps,
		Limits:       &wire.Limits{MaxHeaderValue: 16},
	})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	var back mq.Message
	uerr := narrow.Unwrap(&back, out)
	if uerr == nil {
		t.Fatalf("an envelope the consumer's limits refuse was delivered as a native body "+
			"of %d bytes: the application receives a payload nobody published", len(back.Body))
	}
	if mq.CodeOf(uerr) != mq.CodeMessageTooLarge {
		t.Fatalf("Unwrap answered %v (%v), want CodeMessageTooLarge: the refusal a limit "+
			"exists to produce", mq.CodeOf(uerr), uerr)
	}
}

// FuzzEnvelopeAdaptive is the target that would have found the defect above.
//
// It is the round trip of MetadataAdaptive on a broker with no headers, over
// bodies the fuzzer is deliberately encouraged to shape like envelopes: whatever
// Wrap produces, Unwrap must reproduce the message that was published, and a
// message adaptive mode declines to envelope must come back with exactly the
// metadata it was given and no more.
//
// FuzzEnvelopeCheck could not have found it. That target runs
// MetadataPortableEnvelope, where every record is an envelope and there is
// nothing to discriminate; the hole is reachable only in the mode that mixes
// the two on one subscription.
func FuzzEnvelopeAdaptive(f *testing.F) {
	seedAdaptive(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		layouts := wire.Kinds()
		layout := layouts[r.Choice(len(layouts))]
		// The body's shape is read HERE rather than after the message, so
		// that a seed can ask for it: read later it would sit past whatever
		// the message's own fields consumed, and an exhausted reader would
		// pad the layout/version byte with a zero no decoder knows — which
		// is how the first version of this target passed its own seeds.
		shape := r.Choice(4)
		// The two layout/version bytes wire.Detect knows. Taken from the
		// fuzzer they would essentially never appear.
		versions := []byte{0x11, 0x01}
		version := versions[r.Choice(len(versions))]
		flags := byte(r.Int(0, 15))

		caps := driverkit.NewCapabilities().Native(mq.FeatureAtLeastOnce, mq.ScopeTopic).MustBuild()
		env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
			Mode:         mq.MetadataAdaptive,
			Layout:       layout,
			Limits:       envelopeLimits(),
			Capabilities: caps,
		})
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		m := fuzzEnvelopeMessage(r)
		switch shape {
		case 0:
			// A body that BEGINS like an envelope.
			m.Body = append([]byte("IQMQ"), version, flags)
			m.Body = append(m.Body, r.Bytes(64)...)
		case 1:
			// A body that IS an envelope, published natively.
			inner := &mq.Message{Body: r.Bytes(64), ID: r.String(8)}
			inner.Headers.Add(r.String(8), r.Bytes(8))
			pe, perr := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
				Mode:   mq.MetadataPortableEnvelope,
				Layout: layout,
				Limits: envelopeLimits(),
			})
			if perr != nil {
				t.Fatalf("NewEnvelope: %v", perr)
			}
			if wrapped, werr := pe.Wrap(inner); werr == nil {
				m.Body = wrapped.Body
			}
		}

		out, werr := env.Wrap(m)
		if werr != nil {
			// Refused before any I/O: nothing was published and no record
			// exists, which is the correct outcome for a message the
			// envelope cannot carry.
			return
		}
		var back mq.Message
		if uerr := env.Unwrap(&back, out); uerr != nil {
			t.Fatalf("adaptive mode admitted a publication it cannot read back: %v\n"+
				"layout=%s shape=%d enveloped=%v body=%d headers=%d",
				uerr, layout, shape, out != m, len(m.Body), m.Headers.Len())
		}
		if !bytes.Equal(back.Body, m.Body) || !bytes.Equal(back.Key, m.Key) || back.ID != m.ID ||
			back.Headers.Len() != m.Headers.Len() || back.Properties != m.Properties {
			t.Fatalf("the adaptive round trip changed the message\n"+
				"layout=%s shape=%d enveloped=%v\n"+
				"sent  id=%q headers=%d key=%d body=%d\n"+
				"back  id=%q headers=%d key=%d body=%d",
				layout, shape, out != m,
				m.ID, m.Headers.Len(), len(m.Key), len(m.Body),
				back.ID, back.Headers.Len(), len(back.Key), len(back.Body))
		}
	})
}

// seedAdaptive commits the inputs that reach the mixed stream, for every layout
// and every body shape: an ordinary body, a body that begins like an envelope,
// and a whole envelope published as a native body.
func seedAdaptive(f *testing.F) {
	// message writes fuzzEnvelopeMessage's reads for a plain message with no
	// metadata at all, which is the only shape adaptive mode leaves native on
	// a header-less broker — and therefore the only shape in which the body
	// has to be told apart from an envelope.
	message := func(w *fuzzutil.Writer, body string) {
		w.Choice(3, 2)
		w.Bytes(512, []byte(body))
		w.Choice(3, 0)
		w.String(64, "t")
		w.String(300, "")
		w.Int(0, 6, 0)
		for range 6 {
			w.Bool(false)
		}
	}
	layouts := len(wire.Kinds())
	for ki := range layouts {
		for shape := range 4 {
			for vi := range 2 {
				w := fuzzutil.NewWriter()
				w.Choice(layouts, ki)
				w.Choice(4, shape)
				w.Choice(2, vi)
				w.Int(0, 15, 0)
				message(w, "payload")
				if shape == 0 {
					w.Bytes(64, []byte("and then some ordinary bytes"))
				}
				if shape == 1 {
					w.Bytes(64, []byte("inner"))
					w.String(8, "inner-1")
					w.String(8, "h")
					w.Bytes(8, []byte("v"))
				}
				f.Add(w.Data())
			}
		}
	}
	// A message that DOES need the envelope, so the seed corpus covers both
	// arms of the adaptive decision.
	w := fuzzutil.NewWriter()
	w.Choice(layouts, 0)
	w.Choice(4, 2)
	w.Choice(2, 0)
	w.Int(0, 15, 0)
	w.Choice(3, 2)
	w.Bytes(512, []byte("payload"))
	w.Choice(3, 2)
	w.Bytes(300, []byte("k"))
	w.String(64, "t")
	w.String(300, "id-1")
	w.Int(0, 6, 2)
	w.String(300, "trace")
	w.Bytes(300, []byte{0x00, 0xff})
	w.String(300, "trace")
	w.Bytes(300, []byte("second"))
	for range 6 {
		w.Bool(false)
	}
	f.Add(w.Data())
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
}
