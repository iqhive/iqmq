package outbox_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/outbox"
)

// FuzzDecodeHeaders: the header blob comes out of a database column, so every
// length in it is a claim. It must never panic, never allocate on a declared
// count, and never produce headers from a blob it rejected.
func FuzzDecodeHeaders(f *testing.F) {
	var h mq.Headers
	h.Add("a", []byte("1"))
	h.Add("a", []byte("2"))
	f.Add([]byte(nil))
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	f.Add(outbox.EncodeHeaders(h))
	f.Add(append(outbox.EncodeHeaders(h), 'x'))
	f.Add([]byte{0, 0, 0, 1, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, blob []byte) {
		got, err := outbox.DecodeHeaders(blob)
		if err != nil {
			if !errors.Is(err, outbox.ErrBadHeaders) {
				t.Fatalf("DecodeHeaders = %v, want ErrBadHeaders", err)
			}
			if got.Len() != 0 {
				t.Fatalf("a refused blob produced %d header(s)", got.Len())
			}
			return
		}
		// A blob that decoded must re-encode to itself, so the two halves
		// cannot drift apart. The nil/empty case encodes to nil, which is
		// the documented representation of "no headers".
		again := outbox.EncodeHeaders(got)
		if got.Len() == 0 {
			if again != nil {
				t.Fatalf("EncodeHeaders of no headers = %x, want nil", again)
			}
			return
		}
		if !bytes.Equal(again, blob) {
			t.Fatalf("EncodeHeaders(DecodeHeaders(b)) = %x, want %x", again, blob)
		}
	})
}

// FuzzRelayNeverMarksWhatItDidNotPublish is the package's whole promise as a
// fuzz invariant, over an op script that chooses each publication's outcome.
//
// The model is one line: a row may be marked sent only if some attempt on it
// returned a definite success. Every other outcome -- a failure, an ambiguous
// outcome, a poison bound, a missing topic -- must leave the row claimable.
func FuzzRelayNeverMarksWhatItDidNotPublish(f *testing.F) {
	f.Add([]byte{0, 0, 0}, uint8(3))
	f.Add([]byte{1, 1, 1}, uint8(3))
	f.Add([]byte{2, 2, 2}, uint8(3))
	f.Add([]byte{0, 1, 2, 3, 0}, uint8(5))
	f.Add([]byte{3, 3, 0}, uint8(2))

	f.Fuzz(func(t *testing.T, script []byte, rowCount uint8) {
		n := int(rowCount)%8 + 1
		s, err := outbox.NewMemoryStore(outbox.MemoryOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = s.Close() }()
		ids := make([]string, 0, n)
		for i := range n {
			id, err := s.Append(outbox.Row{Topic: "t", Body: []byte{byte(i)}})
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}

		// publishedOK[id] is the model: did any attempt definitely succeed?
		publishedOK := map[string]bool{}
		var outcome uint8
		sk := func(_ context.Context, _ string, ms []*mq.Message, _ []mq.PublishOption) error {
			switch outcome % 4 {
			case 0: // success
				for _, m := range ms {
					publishedOK[ids[int(m.Body[0])]] = true
				}
				return nil
			case 1: // plain failure
				return errors.New("refused")
			case 2: // ambiguous by code
				return ambiguousByCode()
			default: // ambiguous by flag
				return ambiguousByFlag()
			}
		}
		r, err := outbox.New(outbox.Config{Store: s, Publish: sk, MaxAttempts: 4})
		if err != nil {
			t.Fatal(err)
		}
		for _, op := range script {
			outcome = op
			if _, err := r.Drain(context.Background()); err != nil {
				t.Fatalf("Drain = %v", err)
			}
			// The invariant, checked after every pass: nothing is marked
			// sent that the model does not say was published.
			for _, id := range s.Sent() {
				if !publishedOK[id] {
					t.Fatalf("row %s is marked sent but no attempt on it definitely succeeded; this is how the outbox pattern loses a message", id)
				}
			}
		}
		// And the converse, so the invariant is not satisfiable by marking
		// nothing: a row that definitely succeeded must end up marked.
		sentSet := map[string]bool{}
		for _, id := range s.Sent() {
			sentSet[id] = true
		}
		for id, ok := range publishedOK {
			if ok && !sentSet[id] {
				t.Fatalf("row %s was published successfully and is not marked sent; it will be published again for ever", id)
			}
		}
	})
}

// FuzzRowMessage: a Row from arbitrary bytes either becomes a message that
// agrees with it, or is refused.
func FuzzRowMessage(f *testing.F) {
	f.Add("", []byte(nil), []byte(nil), "")
	f.Add("topic", []byte("k"), []byte("b"), "id")
	f.Add("", []byte("k"), []byte("b"), "id")

	f.Fuzz(func(t *testing.T, topic string, key, body []byte, id string) {
		row := outbox.Row{Topic: topic, Key: key, Body: body, MessageID: id}
		m, err := row.Message()
		if err != nil {
			if topic != "" {
				t.Fatalf("Message = %v for a row with topic %q", err, topic)
			}
			if !errors.Is(err, outbox.ErrNoTopic) {
				t.Fatalf("Message = %v, want ErrNoTopic", err)
			}
			if m != nil {
				t.Fatal("Message failed and returned a message")
			}
			return
		}
		if topic == "" {
			t.Fatal("Message accepted a row with no topic; a default topic delivers somebody's orders to the wrong place quietly")
		}
		if m.Topic != topic || m.ID != id || !bytes.Equal(m.Key, key) || !bytes.Equal(m.Body, body) {
			t.Fatalf("Message = %+v, want the row's own fields", m)
		}
	})
}

// varint keeps the import of encoding/binary honest if a future seed needs
// to build a blob by hand; it is used by the seeds above only indirectly.
var _ = binary.BigEndian
