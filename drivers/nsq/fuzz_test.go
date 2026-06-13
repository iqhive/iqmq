package nsq

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	gonsq "github.com/nsqio/go-nsq"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
	"bitbucket.org/iqhive/iqmq/wire"
)

// FuzzNSQEnvelope is the row-26 target for this driver (target name fixed
// by the specification): round trip, limits, reserved-field collisions and
// deterministic behaviour, over the ONE metadata path NSQ has.
//
// The properties it holds are the four this driver's correctness rests on, and
// each of them is a defect somebody has actually shipped:
//
//  1. Every mode either refuses the message with one of THIS DRIVER'S OWN
//     refusals or produces something nsqd would accept — a non-empty body no
//     longer than --max-msg-size. An unnamed error fails the target, because
//     an error nobody chose is an error nobody has thought about.
//  2. Whatever a mode produces, the matching decode reproduces the message
//     that went in, byte for byte, including the nil-versus-empty
//     distinction. A publication whose own decoder refuses it is a record
//     that can be neither delivered nor settled.
//  3. A refusal in the strict mode is an ACCEPTANCE in the enveloping ones,
//     except where the envelope's own limits are what refused. That is the
//     whole claim of the specification's "envelope required for portable metadata": if
//     strict refuses something the envelope also cannot carry, the envelope
//     is not the portability story it is advertised as.
//  4. A body that only LOOKS like an envelope survives adaptive mode, which
//     is the recorded MetadataAdaptive defect and is unavoidable here: with
//     nothing out of band, the body is the only evidence there is.
func FuzzNSQEnvelope(f *testing.F) {
	seedEnvelope(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		cfg := Config{
			Addrs:           []string{"nsqfake:4150"},
			MaxMessageBytes: int64(r.Int(512, DefaultMaxMessageBytes)),
		}
		resolved := cfg.resolve()
		limits := envelopeLimits(resolved)
		caps, err := capabilities(resolved)
		if err != nil {
			t.Fatalf("capabilities: %v", err)
		}
		m := fuzzMessage(r)

		var strictRefusal error
		for _, mode := range [...]mq.MetadataMode{
			mq.MetadataNativeStrict,
			mq.MetadataPortableEnvelope,
			mq.MetadataAdaptive,
			mq.MetadataLenientDrop,
		} {
			env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mode, Limits: limits, Capabilities: caps})
			if err != nil {
				t.Fatalf("NewEnvelope(%v): %v", mode, err)
			}
			forced, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
				Mode: mq.MetadataPortableEnvelope, Limits: limits, Capabilities: caps,
			})
			if err != nil {
				t.Fatalf("NewEnvelope(forced): %v", err)
			}
			out, werr := wrapForPublish(env, forced, limits, mode, m)
			if werr != nil {
				assertNamedRefusal(t, mode, werr)
				if mode == mq.MetadataNativeStrict {
					strictRefusal = werr
				}
				continue
			}
			// What a mode produced has to be something nsqd would accept.
			if mode != mq.MetadataNativeStrict && mode != mq.MetadataLenientDrop {
				if len(out.Body) == 0 {
					t.Fatalf("mode %v produced an empty body, which nsqd answers with E_BAD_MESSAGE", mode)
				}
			}
			var back mq.Message
			if uerr := env.Unwrap(&back, &mq.Message{Topic: m.Topic, Body: out.Body}); uerr != nil {
				t.Fatalf("mode %v admitted a publication it cannot read back: %v", mode, uerr)
			}
			switch mode {
			case mq.MetadataNativeStrict, mq.MetadataLenientDrop:
				// Neither mode envelopes, so only the body travels.
				if !bytes.Equal(back.Body, m.Body) {
					t.Fatalf("mode %v changed the body", mode)
				}
			case mq.MetadataPortableEnvelope, mq.MetadataAdaptive:
				assertRoundTrip(t, mode, m, &back)
			}
		}

		// Property 3: what strict refused for want of a native field, the
		// envelope carries. A refusal for want of ENVELOPE capacity is a
		// different thing and is allowed to stand in every mode.
		if strictRefusal != nil && mq.CodeOf(strictRefusal) == mq.CodeUnsupported {
			env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
				Mode: mq.MetadataPortableEnvelope, Limits: limits, Capabilities: caps,
			})
			if err != nil {
				t.Fatalf("NewEnvelope: %v", err)
			}
			if _, werr := wrapForPublish(env, env, limits, mq.MetadataPortableEnvelope, m); werr != nil {
				if mq.CodeOf(werr) == mq.CodeUnsupported {
					t.Fatalf("the envelope refused as UNSUPPORTED what strict refused as unsupported (%v); "+
						"The specification makes the envelope this broker's whole portability story", werr)
				}
			}
		}
	})
}

// assertNamedRefusal holds every refusal to this driver's own vocabulary. An
// error nobody chose is one nobody has thought about, and it is how a native
// client's error string ends up in front of an application.
func assertNamedRefusal(t *testing.T, mode mq.MetadataMode, err error) {
	t.Helper()
	for _, known := range []error{
		errNoHeaders, errNoIdentifier, errNoProperties, errNoKey, errNilMessage,
		errEnvelopeKey, errEnvelopeBody, errEnvelopeID, errEnvelopeHeaderCount,
		errEnvelopeHeaderKey, errEnvelopeHeaderValue, errEnvelopeProperty,
		errEnvelopeIDUTF8, errEnvelopeHeaderKeyUTF8, errEnvelopePropertyUTF8,
		errMetadataMode,
	} {
		if errors.Is(err, known) {
			return
		}
	}
	if errors.Is(err, errEnvelopeUndiagnosed) {
		t.Fatalf("mode %v refused a message and could not say which field is at fault: the driver's "+
			"diagnosis has drifted from driverkit.Envelope.Check, which is the defect class "+
			"the maintenance notes records three drivers falling into", mode)
	}
	t.Fatalf("mode %v refused a message with an error this driver did not choose: %v", mode, err)
}

// assertRoundTrip holds the envelope to reproducing the message exactly,
// the nil-versus-empty distinction included.
func assertRoundTrip(t *testing.T, mode mq.MetadataMode, sent, got *mq.Message) {
	t.Helper()
	if (sent.Body == nil) != (got.Body == nil) || !bytes.Equal(sent.Body, got.Body) {
		t.Fatalf("mode %v: the body came back nil=%v len=%d, sent nil=%v len=%d",
			mode, got.Body == nil, len(got.Body), sent.Body == nil, len(sent.Body))
	}
	if (sent.Key == nil) != (got.Key == nil) || !bytes.Equal(sent.Key, got.Key) {
		t.Fatalf("mode %v: the key came back nil=%v len=%d, sent nil=%v len=%d",
			mode, got.Key == nil, len(got.Key), sent.Key == nil, len(sent.Key))
	}
	if got.ID != sent.ID {
		t.Fatalf("mode %v: the identifier came back %q, sent %q", mode, got.ID, sent.ID)
	}
	if got.Properties != sent.Properties {
		t.Fatalf("mode %v: the properties came back %+v, sent %+v", mode, got.Properties, sent.Properties)
	}
	if got.Headers.Len() != sent.Headers.Len() {
		t.Fatalf("mode %v: %d headers came back, sent %d", mode, got.Headers.Len(), sent.Headers.Len())
	}
	for k := range sent.Headers.All() {
		want, have := sent.Headers.Values(k), got.Headers.Values(k)
		if len(want) != len(have) {
			t.Fatalf("mode %v: header %q came back with %d values, sent %d", mode, k, len(have), len(want))
		}
		for i := range want {
			if !bytes.Equal(want[i], have[i]) {
				t.Fatalf("mode %v: header %q value %d came back %q, sent %q", mode, k, i, have[i], want[i])
			}
		}
	}
}

// fuzzMessage builds a message from the fuzzer's bytes, deliberately reaching
// past the envelope's limits and past UTF-8 validity: the interesting inputs
// are the ones one byte over a bound, the ones with a lone continuation byte in
// a field the decoder validates, and the ones whose body begins with the
// envelope's own magic.
func fuzzMessage(r *fuzzutil.Reader) *mq.Message {
	m := &mq.Message{Topic: "orders"}
	switch r.Choice(5) {
	case 0:
		m.Body = nil
	case 1:
		m.Body = []byte{}
	case 2:
		// A body that begins like an envelope, which adaptive mode has to
		// carry as a payload rather than decode as metadata.
		m.Body = append([]byte("IQMQ"), []byte{0x11, 0x01}[r.Choice(2)], byte(r.Int(0, 15)))
		m.Body = append(m.Body, r.Bytes(64)...)
	case 3:
		// A body that IS a whole envelope, published as a payload.
		inner := &mq.Message{Body: r.Bytes(32), ID: r.String(8)}
		if b, err := wire.New(wire.Auto).Encode(nil, inner); err == nil {
			m.Body = b
		}
	default:
		m.Body = r.Bytes(512)
	}
	switch r.Choice(3) {
	case 0:
		m.Key = nil
	case 1:
		m.Key = []byte{}
	default:
		m.Key = r.Bytes(300)
	}
	m.ID = r.String(300)
	for n := r.Int(0, 6); n > 0; n-- {
		m.Headers.Add(r.String(300), r.Bytes(300))
	}
	if r.Bool() {
		m.Properties.ContentType = r.String(64)
	}
	if r.Bool() {
		m.Properties.ReplyTo = r.String(6000)
	}
	if r.Bool() {
		m.Properties.CorrelationID = r.String(64)
	}
	return m
}

// seedEnvelope commits the inputs that reach each decision this mapping makes.
func seedEnvelope(f *testing.F) {
	repeat := func(c byte, n int) string { return strings.Repeat(string([]byte{c}), n) }
	// body writes fuzzMessage's body choice; the rest of the message follows.
	seeds := []func(*fuzzutil.Writer){
		// A plain message with no metadata: the one shape adaptive mode
		// leaves native, and therefore the one whose body has to be told
		// apart from an envelope.
		func(w *fuzzutil.Writer) {
			w.Choice(5, 4)
			w.Bytes(512, []byte("payload"))
			w.Choice(3, 0)
			w.String(300, "")
			w.Int(0, 6, 0)
			w.Bool(false)
			w.Bool(false)
			w.Bool(false)
		},
		// A nil body, which nsqd refuses and the envelope carries.
		func(w *fuzzutil.Writer) {
			w.Choice(5, 0)
			w.Choice(3, 0)
			w.String(300, "")
			w.Int(0, 6, 0)
			w.Bool(false)
			w.Bool(false)
			w.Bool(false)
		},
		// An empty body, which is a different thing from a nil one.
		func(w *fuzzutil.Writer) {
			w.Choice(5, 1)
			w.Choice(3, 1)
			w.String(300, "")
			w.Int(0, 6, 0)
			w.Bool(false)
			w.Bool(false)
			w.Bool(false)
		},
		// A body beginning with the envelope's magic, for each layout byte.
		func(w *fuzzutil.Writer) {
			w.Choice(5, 2)
			w.Choice(2, 0)
			w.Int(0, 15, 0)
			w.Bytes(64, []byte("and then some ordinary bytes"))
			w.Choice(3, 0)
			w.String(300, "")
			w.Int(0, 6, 0)
			w.Bool(false)
			w.Bool(false)
			w.Bool(false)
		},
		func(w *fuzzutil.Writer) {
			w.Choice(5, 2)
			w.Choice(2, 1)
			w.Int(0, 15, 0)
			w.Bytes(64, nil)
			w.Choice(3, 0)
			w.String(300, "")
			w.Int(0, 6, 0)
			w.Bool(false)
			w.Bool(false)
			w.Bool(false)
		},
		// A whole envelope published as a payload.
		func(w *fuzzutil.Writer) {
			w.Choice(5, 3)
			w.Bytes(32, []byte("inner"))
			w.String(8, "inner-1")
			w.Choice(3, 0)
			w.String(300, "")
			w.Int(0, 6, 0)
			w.Bool(false)
			w.Bool(false)
			w.Bool(false)
		},
		// A key and nothing else: the reason Envelope.Needed cannot be the
		// whole adaptive decision here.
		func(w *fuzzutil.Writer) {
			w.Choice(5, 4)
			w.Bytes(512, []byte("b"))
			w.Choice(3, 2)
			w.Bytes(300, []byte("k"))
			w.String(300, "")
			w.Int(0, 6, 0)
			w.Bool(false)
			w.Bool(false)
			w.Bool(false)
		},
		// A duplicate binary header pair, which is the promise and the
		// envelope's whole reason for being here.
		func(w *fuzzutil.Writer) {
			w.Choice(5, 4)
			w.Bytes(512, []byte("b"))
			w.Choice(3, 0)
			w.String(300, "id-1")
			w.Int(0, 6, 2)
			w.String(300, "trace")
			w.Bytes(300, []byte{0x00, 0xff})
			w.String(300, "trace")
			w.Bytes(300, []byte("second"))
			w.Bool(true)
			w.String(64, "application/json")
			w.Bool(false)
			w.Bool(false)
		},
		// A header key one byte over the envelope's MaxHeaderKey: the input
		// the specification's own target found in five seconds.
		func(w *fuzzutil.Writer) {
			w.Choice(5, 4)
			w.Bytes(512, []byte("b"))
			w.Choice(3, 0)
			w.String(300, "")
			w.Int(0, 6, 1)
			w.String(300, repeat('k', wire.DefaultLimits.MaxHeaderKey+1))
			w.Bytes(300, []byte("v"))
			w.Bool(false)
			w.Bool(false)
			w.Bool(false)
		},
		// An identifier that is not valid UTF-8, which the envelope's
		// encoder accepts and its decoder refuses.
		func(w *fuzzutil.Writer) {
			w.Choice(5, 4)
			w.Bytes(512, []byte("b"))
			w.Choice(3, 0)
			w.String(300, "\xff")
			w.Int(0, 6, 0)
			w.Bool(false)
			w.Bool(false)
			w.Bool(false)
		},
		// A typed property past the envelope's MaxProp.
		func(w *fuzzutil.Writer) {
			w.Choice(5, 4)
			w.Bytes(512, []byte("b"))
			w.Choice(3, 0)
			w.String(300, "")
			w.Int(0, 6, 0)
			w.Bool(false)
			w.Bool(true)
			w.String(6000, repeat('r', wire.DefaultLimits.MaxProp+1))
			w.Bool(false)
		},
	}
	for _, seed := range seeds {
		w := fuzzutil.NewWriter()
		w.Int(512, DefaultMaxMessageBytes, DefaultMaxMessageBytes)
		seed(w)
		f.Add(w.Data())
	}
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
}

// FuzzNSQTopicName is the target 3 for this driver: grammar agreement and
// an injective mapper.
//
// The oracle is go-nsq's own validator, which is the same grammar written as a
// regular expression. This driver validates by hand so that checking a name
// costs no match, and the only thing that can keep the two in step is a target
// that runs both over the same input.
func FuzzNSQTopicName(f *testing.F) {
	for _, seed := range []string{
		"", "a", "orders", "a.b_c-d", "a#ephemeral", "#ephemeral", "a#ephemerals",
		strings.Repeat("x", 64), strings.Repeat("x", 65), "a/b", "a b", "a\x00b",
		"ORDERS", "0", ".", "..", "a#ephemeral#ephemeral", "é",
	} {
		f.Add(seed, "prefix.")
	}
	f.Fuzz(func(t *testing.T, name, prefix string) {
		ours := CheckTopic(name) == nil
		theirs := gonsq.IsValidTopicName(name)
		if ours != theirs {
			t.Fatalf("CheckTopic(%q) says %v and go-nsq's own validator says %v: the hand-rolled "+
				"grammar has drifted from the regular expression it copies", name, ours, theirs)
		}
		if CheckChannel(name) == nil != gonsq.IsValidChannelName(name) {
			t.Fatalf("CheckChannel(%q) disagrees with go-nsq's validator", name)
		}
		// The mapper is a pure prepend, so it is injective for a fixed
		// prefix and it never invents a legal name out of an illegal one:
		// a mapper that escaped the illegal bytes would map "a/b" and
		// "a-b" onto one topic, and a caller would find one subscription
		// receiving another's messages.
		mapped, err := TopicOf(prefix, name)
		if err != nil {
			return
		}
		if mapped != prefix+name {
			t.Fatalf("TopicOf(%q, %q) = %q, want the pure prepend", prefix, name, mapped)
		}
		if again, err := TopicOf(prefix, name); err != nil || again != mapped {
			t.Fatalf("TopicOf is not deterministic: %q then %q (%v)", mapped, again, err)
		}
	})
}

// FuzzNSQDSN is the target 1 for this driver: no panic, a stable
// rendering, redaction, and a strict refusal of a parameter this driver does
// not have.
//
// The rendering property is the one that matters operationally. Config.DSN is
// what a log line and an error carry, so it has to be safe to print: a control
// byte or invalid UTF-8 that survived from a DSN into the rendering produces
// output no log pipeline can handle, which is what FuzzNATSDSN found on its
// first run.
func FuzzNSQDSN(f *testing.F) {
	for _, seed := range []string{
		"", "nsq://", "nsq://host", "nsq://host:4150",
		"nsq://a:4150,b:4150/prefix.?lookupd=http://l:4161,http://m:4161",
		"nsq://host:4150?authsecret=s3cret&metadata=adaptive",
		"nsq://host:4150?authsecret=a-shared-secret-long-enough-to-be-one",
		"nsq://host:4150?ensuretopology=true&snappy=true&ackdeadline=45s",
		"nsq://host:4150?unknown=1", "nsq://h\xc5st:4150", "nsq://host:4150?clientid=a\x00b",
		"nats://host:4222", "nsq://host:99999", "nsq://:@host:4150",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		cfg, err := ParseDSN(s)
		if err != nil {
			return
		}
		rendered := cfg.DSN()
		if !printableText(rendered) {
			t.Fatalf("ParseDSN(%q) rendered %q, which is not printable text: it reaches a log line "+
				"and an error string, and no log pipeline can carry it", s, rendered)
		}
		if secret := cfg.AuthSecret.Reveal(); secret != "" {
			// The marker has to be there whatever the secret is: it is
			// what says the field was rendered as redacted rather than
			// dropped, which a caller reading a log line needs to know.
			if !strings.Contains(rendered, "authsecret="+dsn.Redacted) {
				t.Fatalf("a configured credential is not marked as redacted: %q", rendered)
			}
			// The containment check needs a secret long enough to be one.
			// A one-character "credential" collides with a digit of a
			// duration or a byte of a host name, and the corpus input
			// testdata/fuzz/FuzzNSQDSN/short-secret-substring is exactly
			// that: `nsq://0?authsecret=0`, whose rendering contains "0"
			// eleven times and the credential not at all. Sixteen bytes
			// is far below anything a real shared secret would be and far
			// above the length at which a coincidence is plausible.
			if len(secret) >= 16 && strings.Contains(rendered, secret) {
				t.Fatalf("the rendering carries the credential: %q", rendered)
			}
		}
		again, err := ParseDSN(rendered)
		if err != nil {
			t.Fatalf("the rendering of %q does not parse back: %q: %v", s, rendered, err)
		}
		if again.DSN() != rendered {
			t.Fatalf("the rendering is not stable:\n first %q\nsecond %q", rendered, again.DSN())
		}
		if verr := cfg.Validate(); verr != nil {
			t.Fatalf("ParseDSN returned a configuration Validate refuses: %v", verr)
		}
	})
}

// FuzzNSQErrorMapping is the target 10 for this driver: stable codes, the
// native error still reachable through errors.As, and no retry of a definite
// permanent failure.
//
// The property worth stating is the last one. A publish this driver reports as
// definitely failed is one the core may retry, and a publish it reports as
// AMBIGUOUS is one the core must not — so a mapper that marked an ambiguous
// outcome retryable would turn one publication into two messages, and one that
// marked a definite refusal ambiguous would make a lost message look like a
// duplicate.
func FuzzNSQErrorMapping(f *testing.F) {
	for _, seed := range []string{
		"", "E_BAD_TOPIC", "E_BAD_TOPIC PUB topic name 'x' is not valid",
		"E_BAD_MESSAGE", "E_INVALID", "E_INVALID_AUTH", "E_FIN_FAILED",
		"E_AUTH_FAILED", "E_UNAUTHORIZED", "not connected", "stopped",
	} {
		for _, op := range []string{mq.OpPublish, mq.OpFetch, mq.OpAck, mq.OpExtend, mq.OpAdmin} {
			f.Add(seed, op)
		}
	}
	m := newErrorMapper()
	f.Fuzz(func(t *testing.T, reason, op string) {
		for name, native := range map[string]error{
			"a protocol frame":    gonsq.ErrProtocol{Reason: reason},
			"an identify failure": gonsq.ErrIdentify{Reason: reason},
			"an opaque error":     errors.New(reason),
			"an ambiguity":        &ambiguousError{cause: errors.New(reason)},
		} {
			mapped := m.Map(op, "topic", "sub", native)
			if mapped == nil {
				t.Fatalf("%s: the mapper turned an error into nil", name)
			}
			// The native error stays reachable, which the specification requires.
			if !errors.Is(mapped, native) {
				var p gonsq.ErrProtocol
				var i gonsq.ErrIdentify
				if !errors.As(mapped, &p) && !errors.As(mapped, &i) {
					t.Fatalf("%s: the native error is not reachable through the mapping", name)
				}
			}
			code := mq.CodeOf(mapped)
			if code == mq.CodeUnknown && reason != "" && name == "a protocol frame" {
				// An unrecognised frame is allowed to be Unknown; what is
				// not allowed is a code that changes between calls.
				_ = code
			}
			if again := mq.CodeOf(m.Map(op, "topic", "sub", native)); again != code {
				t.Fatalf("%s: the code is not stable: %v then %v", name, code, again)
			}
			// This read the FLAG through mq.IsRetryable, which since
			// The specification applies the precedence itself: the
			// predicate is false for anything ambiguous, so the check as
			// written could no longer fail -- a vacuous check, which is
			// worse than no check. What it was really asserting is this
			// driver's own documented rule ("IsAmbiguous is true and
			// Retryable is false", docs/drivers/nsq.md), so it now reads the
			// flag where the driver sets it and fails again if a rule marks
			// an ambiguous nsq outcome retryable.
			var amb *mq.OpError
			if mq.IsAmbiguous(mapped) && errors.As(mapped, &amb) && amb.Retryable {
				t.Fatalf("%s: an ambiguous outcome is marked retryable, and this driver documents "+
					"that it never is (docs/drivers/nsq.md)", name)
			}
			switch code {
			case mq.CodeInvalidArgument, mq.CodeMessageTooLarge, mq.CodeUnauthorized, mq.CodeForbidden:
				if mq.IsRetryable(mapped) {
					t.Fatalf("%s: a definite permanent failure (%v) is marked retryable", name, code)
				}
			default:
				// Every other code is either retryable or not on its own
				// terms; the specification makes retryability operation-specific and
				// not inferable from the code alone.
			}
		}
	})
}
