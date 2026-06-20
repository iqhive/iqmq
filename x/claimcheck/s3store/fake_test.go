package s3store_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// fakeS3 is the smallest S3 that path-style PutObject, GetObject and
// DeleteObject need, served over real HTTP so that the SDK's own request
// signing, response parsing and error deserialisation are what the store is
// tested through. What it answers is set per test: a missing key is S3's
// NoSuchKey, and fail makes every request answer with a chosen status and
// error code instead.
type fakeS3 struct {
	srv *httptest.Server

	mu      sync.Mutex
	objects map[string][]byte // "bucket/key" -> body
	failing *fakeFailure

	requests atomic.Int64
	// chunked serves GET bodies with no Content-Length, in chunks, the way
	// a proxy or a server streaming an object may: the store then has no
	// declared length to refuse on and must bound the read itself.
	chunked atomic.Bool
}

type fakeFailure struct {
	status int
	code   string
}

func newFakeS3(tb testing.TB) *fakeS3 {
	tb.Helper()
	f := &fakeS3{objects: map[string][]byte{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	tb.Cleanup(f.srv.Close)
	return f
}

// client returns an SDK client for the fake: path-style, one attempt (so an
// injected 503 is answered once rather than retried), and request checksums
// only where S3 requires them, which is the setting an S3-compatible server
// may need and the one this package's documentation names.
func (f *fakeS3) client() *s3.Client {
	return newClient(f.srv.URL, "iqmq", "iqmq-secret")
}

func newClient(endpoint, user, password string) *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint:               aws.String(endpoint),
		UsePathStyle:               true,
		Region:                     "us-east-1",
		Credentials:                credentials.NewStaticCredentialsProvider(user, password, ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
		RetryMaxAttempts:           1,
	})
}

func (f *fakeS3) fail(status int, code string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failing = &fakeFailure{status: status, code: code}
}

func (f *fakeS3) recover() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failing = nil
}

func (f *fakeS3) remove(bucket, key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, bucket+"/"+key)
}

// objectsForTest places an object the store did not write, under a key of
// the test's choosing.
func (f *fakeS3) objectsForTest(key string, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[bucket+"/"+key] = body
}

func (f *fakeS3) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.objects))
	for k := range f.objects {
		out = append(out, k)
	}
	return out
}

func (f *fakeS3) serve(w http.ResponseWriter, r *http.Request) {
	f.requests.Add(1)
	f.mu.Lock()
	failing := f.failing
	f.mu.Unlock()
	if failing != nil {
		writeError(w, failing.status, failing.code)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/")
	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "IncompleteBody")
			return
		}
		f.mu.Lock()
		f.objects[path] = body
		f.mu.Unlock()
		w.Header().Set("ETag", `"fake"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		f.mu.Lock()
		body, ok := f.objects[path]
		f.mu.Unlock()
		if !ok {
			writeError(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		if f.chunked.Load() {
			w.WriteHeader(http.StatusOK)
			for len(body) > 0 {
				n := min(len(body), 64<<10)
				if _, err := w.Write(body[:n]); err != nil {
					return
				}
				if fl, ok := w.(http.Flusher); ok {
					fl.Flush()
				}
				body = body[n:]
			}
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	case http.MethodDelete:
		f.mu.Lock()
		delete(f.objects, path)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed")
	}
}

func writeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>`+
		`<Error><Code>`+code+`</Code><Message>injected by the fake</Message><RequestId>fake</RequestId></Error>`)
}
