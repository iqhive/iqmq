package memory

import (
	"bytes"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestZeroMessageClearsAsFarAsTheStorageIsTheEngines pins both halves of
// ZeroOnRelease's scrub. A copy the engine made is the engine's to the end of
// every slice's capacity, so the bytes past each length are cleared too; an
// owned message is the caller's storage on loan, so only the bytes inside
// each length are, and whatever its slices reach past them is left alone.
// The public API cannot tell the first half from a scrub within the length,
// because Clone's spare capacity starts out zero, so it is pinned here.
func TestZeroMessageClearsAsFarAsTheStorageIsTheEngines(t *testing.T) {
	for _, copied := range []bool{true, false} {
		key, body, trace := []byte("keyKEY"), []byte("bodyBODY"), []byte("traceTRACE")
		m := &mq.Message{Key: key[:3], Body: body[:4]}
		m.Headers.Set("trace", trace[:5])
		zeroMessage(m, copied)
		if m.Key != nil || m.Body != nil || m.Headers.Len() != 0 {
			t.Errorf("copied=%v: the message was not reset", copied)
		}
		for _, s := range []struct {
			b    []byte
			used int
		}{{key, 3}, {body, 4}, {trace, 5}} {
			if !bytes.Equal(s.b[:s.used], make([]byte, s.used)) {
				t.Errorf("copied=%v: %q was not cleared within its length", copied, s.b)
			}
			past := bytes.Equal(s.b[s.used:], make([]byte, len(s.b)-s.used))
			if past != copied {
				t.Errorf("copied=%v: %q, the bytes past the length cleared = %v, want %v", copied, s.b, past, copied)
			}
		}
	}
}
