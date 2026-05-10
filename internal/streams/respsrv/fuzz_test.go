package respsrv_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/streams/fakestreams"
	"bitbucket.org/iqhive/iqmq/internal/streams/respsrv"
)

// FuzzRESPRequest drives arbitrary bytes at the request reader.
//
// It is the fuzz target of a package whose whole job is to frame bytes a peer
// controls into commands, and the two failure modes it exists to find are the
// ones a hand-written case does not reach: a length the peer sent being
// believed (a slice allocated from it, or an index into a slice shorter than
// it), and a framing mistake that leaves the connection alive but
// desynchronised. The first is a panic and the second is a hang, so the target
// bounds both: the handler must return, and it must never panic.
func FuzzRESPRequest(f *testing.F) {
	f.Add([]byte("*1\r\n$4\r\nPING\r\n"))
	f.Add([]byte("*2\r\n$4\r\nXLEN\r\n$1\r\ns\r\n"))
	f.Add([]byte("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n"))
	f.Add([]byte("PING\r\n"))
	f.Add([]byte("*-1\r\n"))
	f.Add([]byte("*1\r\n$-1\r\n"))
	f.Add([]byte("*1\r\n$999999999999999999999\r\n"))
	f.Add([]byte("*99999999999999\r\n"))
	f.Add([]byte("*1\r\n$4\r\nPING"))
	f.Add([]byte("*7\r\n$5\r\nCLIENT\r\n"))
	f.Add([]byte("*1\r\n$6\r\nCLIENT\r\n"))
	f.Add([]byte("*1\r\n$5\r\nHELLO\r\n"))
	f.Add([]byte{0})
	f.Add([]byte("\r\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		client, server := net.Pipe()
		srv := fakestreams.New(fakestreams.Options{AOF: true})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			// The handler's own error is not asserted on: a malformed input
			// is SUPPOSED to be refused. What is asserted is that it returns.
			_ = respsrv.HandleConn(ctx, srv, respsrv.Options{MaxTokens: 64, MaxBulk: 4096}, server)
			_ = server.Close()
		}()
		deadline := time.Now().Add(5 * time.Second)
		_ = client.SetDeadline(deadline)
		// Write and drain concurrently: a net.Pipe is unbuffered, so a
		// request whose reply is larger than the peer reads would deadlock
		// the writer against the reader if either half were sequential.
		wrote := make(chan struct{})
		go func() {
			defer close(wrote)
			_, _ = client.Write(data)
			_ = client.Close()
		}()
		_, _ = io.Copy(io.Discard, client)
		<-wrote
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("the connection handler did not return for %q", data)
		}
	})
}
