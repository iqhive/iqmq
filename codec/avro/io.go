package avro

import avro "github.com/hamba/avro/v2"

// encState is one pooled encoder and the writer it is bound to. They are
// pooled together because the encoder holds a reference to its writer: a
// pooled encoder pointing at a writer another goroutine has taken is how two
// publications come to share a buffer.
type encState struct {
	e *avro.Encoder
	w appendWriter
}

// appendWriter is an io.Writer that appends to a caller-owned slice.
type appendWriter struct{ b []byte }

// Write implements io.Writer.
func (w *appendWriter) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}
