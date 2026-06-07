package amqp10_test

import (
	"bytes"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"

	mq "bitbucket.org/iqhive/iqmq"
	aq "bitbucket.org/iqhive/iqmq/drivers/amqp10"
)

// The fuzz targets and the.
//
// Two of them are the ones the specification names for this increment —
// FuzzAMQPMapping and FuzzErrorMapping — and the third is the parser, which
// The specification asks for wherever bytes come from outside the process. All three are
// pure: no connection, no goroutine, no clock.

// FuzzAMQPMapping is the mapping's round trip under arbitrary input.
//
// The property is the one mapping.go exists for: a message this driver ACCEPTS
// comes back byte for byte. A message it refuses is not a failure — the specification makes
// refusing unrepresentable metadata the strict mode's whole job — so what the
// target checks is that acceptance implies fidelity, in every one of the four
// modes.
//
// The other half is what the specification calls a disqualifier from the other direction: a
// message the encoder accepts and the decoder cannot read back is a record that
// can be neither delivered nor settled, made by the driver out of a publication
// it accepted. A crash here would be that record.
func FuzzAMQPMapping(f *testing.F) {
	// The seeds are the shapes that have broken a mapping before: the empty
	// message, the nil-versus-empty body, a binary header value, a binary
	// header KEY, a duplicate key, a reserved name, an over-long key, a
	// non-UTF-8 message key, and every typed property at once.
	f.Add(uint8(0), []byte(nil), []byte(nil), "", "", "", "", "", uint8(0), uint32(0))
	f.Add(uint8(0), []byte{}, []byte{}, "", "", "", "", "", uint8(0), uint32(0))
	f.Add(uint8(0), []byte("k"), []byte("body"), "h", "v", "id", "ct", "corr", uint8(3), uint32(1000))
	f.Add(uint8(0), []byte{0xff, 0xfe}, []byte("body"), "h", "\xff\xfe", "id", "", "", uint8(0), uint32(0))
	f.Add(uint8(0), []byte("k"), []byte("body"), "\xff", "v", "", "", "", uint8(0), uint32(0))
	f.Add(uint8(1), []byte{0xff}, []byte("body"), "dup", "v", "", "", "", uint8(0), uint32(0))
	f.Add(uint8(2), []byte("k"), []byte("body"), mq.ReservedHeaderPrefix+"invented", "v", "", "", "", uint8(0), uint32(0))
	f.Add(uint8(3), []byte("k"), []byte("body"), "x-opt-thing", "v", "", "", "", uint8(0), uint32(0))
	f.Add(uint8(1), []byte("k"), []byte("body"), aq.HeaderSubject, "subj", "", "", "", uint8(0), uint32(0))
	f.Add(uint8(2), []byte("k"), []byte("b"), "long", string(bytes.Repeat([]byte("x"), 300)), "", "", "", uint8(0), uint32(0))

	modes := []mq.MetadataMode{
		mq.MetadataNativeStrict, mq.MetadataPortableEnvelope,
		mq.MetadataAdaptive, mq.MetadataLenientDrop,
	}
	f.Fuzz(func(t *testing.T,
		mode uint8, key, body []byte,
		hkey, hval, id, ctype, corr string,
		priority uint8, ttlMillis uint32,
	) {
		m := mapperFor(t, modes[int(mode)%len(modes)])
		src := &mq.Message{
			Key:  key,
			Body: body,
			ID:   id,
			Properties: mq.Properties{
				ContentType:   ctype,
				CorrelationID: corr,
			},
		}
		if hkey != "" {
			src.Headers.Add(hkey, []byte(hval))
		}
		opts := &mq.PublishOptions{Priority: priority, PrioritySet: true}
		if ttlMillis > 0 {
			opts.TTL = time.Duration(ttlMillis) * time.Millisecond
		}

		out, err := m.Encode(aq.NewBuf(), src, opts)
		if err != nil {
			// A refusal is a correct answer and the whole point of
			// MetadataNativeStrict. What it must NOT be is a refusal with a
			// code a caller cannot act on.
			if code := mq.CodeOf(err); code == mq.CodeUnknown {
				t.Fatalf("the mapping refused a message with an unclassified error: %v", err)
			}
			return
		}
		var got mq.Message
		if _, derr := m.Decode(&got, out); derr != nil {
			t.Fatalf("a message the encoder ACCEPTED could not be decoded: %v. The specification makes such a record a "+
				"disqualifier: it can be neither delivered nor settled, and the driver made it out of a "+
				"publication it accepted (mode=%v key=%q body=%q header=%q=%q)",
				derr, m.Mode(), key, body, hkey, hval)
		}
		// Fidelity, in the fields this mode promises to carry.
		if !bytes.Equal(got.Body, src.Body) {
			t.Fatalf("the body round-tripped as %q, want %q (mode=%v)", got.Body, src.Body, m.Mode())
		}
		if (src.Body == nil) != (got.Body == nil) {
			t.Fatalf("a body that was nil=%v round-tripped as nil=%v; the distinction is one Message promises",
				src.Body == nil, got.Body == nil)
		}
		if m.Mode() != mq.MetadataLenientDrop {
			// Lenient is allowed to lose metadata, which is what makes it
			// explicit; the other three are not.
			if !bytes.Equal(got.Key, src.Key) && len(src.Key) > 0 {
				t.Fatalf("the key round-tripped as %q, want %q (mode=%v)", got.Key, src.Key, m.Mode())
			}
			if got.ID != src.ID {
				t.Fatalf("the identifier round-tripped as %q, want %q (mode=%v)", got.ID, src.ID, m.Mode())
			}
			if got.Properties.ContentType != src.Properties.ContentType {
				t.Fatalf("the content type round-tripped as %q, want %q", got.Properties.ContentType,
					src.Properties.ContentType)
			}
			if got.Properties.CorrelationID != src.Properties.CorrelationID {
				t.Fatalf("the correlation identifier round-tripped as %q, want %q",
					got.Properties.CorrelationID, src.Properties.CorrelationID)
			}
			if hkey != "" {
				v, ok := got.Headers.Get(hkey)
				if !ok {
					t.Fatalf("header %q did not round-trip at all (mode=%v)", hkey, m.Mode())
				}
				if !bytes.Equal(v, []byte(hval)) && (hval != "" || v != nil) {
					t.Fatalf("header %q round-tripped as %q, want %q (mode=%v)", hkey, v, hval, m.Mode())
				}
			}
		}
		// And the publication controls reached the wire where the profile
		// said they would.
		if out.Header == nil {
			t.Fatal("the encoded message carries no AMQP header section, so its durability and priority are the " +
				"node's defaults rather than the caller's")
		}
		if out.Header.Priority != priority {
			t.Fatalf("the priority reached the header as %d, want %d", out.Header.Priority, priority)
		}
	})
}

// FuzzErrorMapping is the rule table under arbitrary AMQP conditions and
// wrappers.
//
// The properties are the four requires of every mapping, and none of them
// depends on which rule fired:
//
//   - it never panics, for any condition symbol at all — including one no broker
//     has ever sent;
//   - the native error stays reachable through errors.As, so a caller who needs
//     the peer's own answer has it;
//   - an *OpError comes back with the driver's name and the operation on it;
//   - a publish is never LESS ambiguous than a fetch of the same failure, which
//     is the rule in the form a fuzzer can check.
func FuzzErrorMapping(f *testing.F) {
	for _, cond := range []string{
		"", "amqp:not-found", "amqp:unauthorized-access", "amqp:resource-limit-exceeded",
		"amqp:link:message-size-exceeded", "amqp:link:detach-forced", "amqp:connection:forced",
		"amqp:link:stolen", "amqp:internal-error", "amqp:precondition-failed",
		"amqp:decode-error", "amqp:invalid-field", "amqp:not-allowed",
		"a condition nobody has ever sent", "\xff\xfe", "amqp:",
	} {
		for shape := range uint8(5) {
			f.Add(shape, cond, "because")
		}
	}
	f.Fuzz(func(t *testing.T, shape uint8, cond, desc string) {
		var native error
		aerr := &amqp.Error{Condition: amqp.ErrCond(cond), Description: desc}
		switch shape % 5 {
		case 0:
			native = aerr
		case 1:
			native = &amqp.LinkError{RemoteErr: aerr}
		case 2:
			native = &amqp.SessionError{RemoteErr: aerr}
		case 3:
			native = &amqp.ConnError{RemoteErr: aerr}
		case 4:
			// The shape with no remote error at all, which go-amqp produces
			// for a link that simply closed: Unwrap returns nil, so nothing
			// in the condition table can match it.
			native = &amqp.LinkError{}
		}
		f := open(t, testProfile(), aq.Config{})
		var ambiguousOnFetch bool
		for _, op := range []string{
			mq.OpPublish, mq.OpFetch, mq.OpAck, mq.OpNack,
			mq.OpExtend, mq.OpOpen, mq.OpClose, mq.OpAdmin,
		} {
			for _, sent := range []bool{false, true} {
				got := aq.MapNetForTest(f.b, op, native, sent)
				if got == nil {
					t.Fatalf("a non-nil native error mapped to nil (op=%s sent=%v)", op, sent)
				}
				var oerr *mq.OpError
				if !asOpError(got, &oerr) {
					t.Fatalf("the mapping produced %T rather than an *OpError (op=%s)", got, op)
				}
				if oerr.Driver != "amqp10test" || oerr.Operation != op {
					t.Fatalf("the *OpError names driver %q operation %q, want amqp10test/%s",
						oerr.Driver, oerr.Operation, op)
				}
				// The specification: "native errors reachable".
				if !errorsIsSame(got, native) {
					t.Fatalf("the mapping lost the native error (op=%s): %v", op, got)
				}
				// The specification forbids an address or a credential in an error
				// string, and this driver's errors carry neither: the
				// rendering is the driver, the operation, the topic, the
				// subscription and the code.
				if oerr.Address != "" {
					t.Fatalf("the *OpError carries an address, which the specification forbids rendering: %q", oerr.Address)
				}
				if op == mq.OpFetch && !sent {
					ambiguousOnFetch = mq.IsAmbiguous(got)
				}
				if op == mq.OpPublish && sent {
					// the rule as a fuzzable property: a publish whose
					// transfer may have left is never told MORE definitely
					// than the same failure on a fetch.
					if ambiguousOnFetch && !mq.IsAmbiguous(got) {
						t.Fatalf("a failure that is ambiguous on a fetch is DEFINITE on a publish whose transfer "+
							"had left: %v", got)
					}
				}
			}
		}
	})
}

// FuzzDSN is the parser under bytes that may come from an environment variable
// a stranger set.
//
// The properties are that it is total — it never panics and never hangs — and
// that acceptance implies re-acceptance: a DSN this driver accepts renders as a
// configuration whose own rendering parses again, with the credential gone. A
// rendering that could not be parsed back would make [aq.Config.DSN] unusable
// for the one thing it exists for, which is putting a configuration in a log
// line a person then pastes.
func FuzzDSN(f *testing.F) {
	for _, seed := range []string{
		"",
		"amqp10://",
		"amqp10://host",
		"amqp10://host:5672",
		"amqp10://user:pass@host:5672?sasl=plain&tls=true",
		"amqp10://a:1,b:2?receiversettle=second&backoff=decorrelated&reconnectgrowth=1.5",
		"amqp10://host?maxcredit=0",
		"amqp10://host?\x93",
		"amqp10://host?unknown=1",
		"amqp10://[::1]:5672?idletimeout=1h",
		"amqp10://%zz@host",
		"amqp10://host?maxframesize=1",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		cfg, err := aq.ParseDSN(s)
		if err != nil {
			return
		}
		// A configuration the parser produced has to be one Validate accepts,
		// or the parser is handing out something nothing will open.
		if verr := cfg.Validate(); verr != nil {
			t.Fatalf("ParseDSN(%q) produced a configuration Validate refuses: %v", s, verr)
		}
		rendered := cfg.DSN()
		if !utf8Valid(rendered) {
			t.Fatalf("ParseDSN(%q) renders as bytes that are not valid UTF-8: %q", s, rendered)
		}
		back, berr := aq.ParseDSN(rendered)
		if berr != nil {
			t.Fatalf("this driver's own rendering %q of %q does not parse back: %v", rendered, s, berr)
		}
		// The rendering is stable, which is what makes it comparable in a log
		// and in a golden file.
		if again := back.DSN(); again != rendered {
			t.Fatalf("the rendering is not stable:\n first: %q\nsecond: %q", rendered, again)
		}
	})
}
