package pulsar_test

import (
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	pd "bitbucket.org/iqhive/iqmq/drivers/pulsar"
)

// What `Representable` refuses, and what a missing identifier answers.
//
// Pulsar carries a key and every header as a STRING, so anything that is not
// valid UTF-8 cannot go on the wire as itself. `Representable` is where that
// is decided, and the strict mode is what turns each of these into a
// refusal rather than an envelope -- so a test of it is a test of what the
// strict mode actually means.

// TestRepresentableRefusesWhatPulsarCannotCarryAsItself.
func TestRepresentableRefusesWhatPulsarCannotCarryAsItself(t *testing.T) {
	t.Parallel()
	withHeader := func(k, v string) *mq.Message {
		m := &mq.Message{Body: []byte("x")}
		m.Headers.Set(k, []byte(v))
		return m
	}

	for _, tc := range []struct {
		name string
		msg  *mq.Message
		ok   bool
	}{
		{"an ordinary message", &mq.Message{Body: []byte("x")}, true},
		{"a message with a text key", &mq.Message{Body: []byte("x"), Key: []byte("customer-42")}, true},
		{"a message with a text header", withHeader("trace", "abc"), true},
		{"no message at all", nil, false},
		{"an EMPTY key that is not nil", &mq.Message{Body: []byte("x"), Key: []byte{}}, false},
		{"a key that is not UTF-8", &mq.Message{Body: []byte("x"), Key: []byte{0xff, 0xfe}}, false},
		{"a header value that is not UTF-8", withHeader("trace", "\xff\xfe"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := pd.Representable(tc.msg)
			if tc.ok && err != nil {
				t.Errorf("Representable is %v, want nil", err)
			}
			if !tc.ok && err == nil {
				t.Error("Representable accepted it; Pulsar carries a key and every header as a STRING, so " +
					"this cannot go on the wire as itself")
			}
		})
	}
}

// TestAnEmptyKeyIsNotTheSameAsNoKey.
//
// A nil key means "no key" and an empty one means "the empty key", which
// Pulsar cannot express: its ordering key is absent or a string, and an empty
// string is absent. Accepting it would silently turn a keyed publication into
// an unkeyed one, which is the ordering guarantee disappearing.
func TestAnEmptyKeyIsNotTheSameAsNoKey(t *testing.T) {
	t.Parallel()

	if err := pd.Representable(&mq.Message{Body: []byte("x")}); err != nil {
		t.Errorf("a message with NO key is %v, want nil", err)
	}
	if err := pd.Representable(&mq.Message{Body: []byte("x"), Key: []byte{}}); err == nil {
		t.Error("a message with an EMPTY key was accepted; Pulsar's ordering key is absent or a string, so " +
			"an empty one would silently become an unkeyed publication")
	}
}

// TestThePositionOfANilIdentifierIsASentinelAndNotAZero.
//
// A zero BatchPosition is a real position -- ledger 0, entry 0 -- so a
// missing identifier has to answer something no message can have. Otherwise a
// caller comparing positions would find the first entry of the first ledger
// equal to "I do not know".
func TestThePositionOfANilIdentifierIsASentinelAndNotAZero(t *testing.T) {
	t.Parallel()

	got := pd.PositionOf(nil)
	if got.Ledger != -1 || got.Entry != -1 || got.Index != -1 {
		t.Errorf("PositionOf(nil) is %+v, want every field -1: a zero is a real position, and a caller "+
			"comparing them would find the first entry of the first ledger equal to \"unknown\"", got)
	}
}
