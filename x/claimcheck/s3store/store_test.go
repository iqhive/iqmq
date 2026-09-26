package s3store_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/claimcheck"
	"github.com/iqhive/iqmq/x/claimcheck/s3store"
	"github.com/iqhive/iqmq/x/claimcheck/storetest"
)

const (
	bucket = "payloads"
	prefix = "claims/"
)

func newStore(tb testing.TB, client *s3.Client) *s3store.Store {
	tb.Helper()
	s, err := s3store.New(s3store.Options{Client: client, Bucket: bucket, Prefix: prefix})
	if err != nil {
		tb.Fatal(err)
	}
	return s
}

// TestContractAgainstAnInProcessS3 holds the store to the contract every
// claim-check store is held to, through the real SDK over real HTTP.
func TestContractAgainstAnInProcessS3(t *testing.T) {
	fakes := map[claimcheck.Store]*fakeS3{}
	storetest.Run(t, storetest.Harness{
		New: func(tb testing.TB) claimcheck.Store {
			f := newFakeS3(tb)
			s := newStore(tb, f.client())
			fakes[s] = f
			return s
		},
		Remove: func(_ testing.TB, s claimcheck.Store, key string) {
			fakes[s].remove(bucket, key)
		},
		Break: func(_ testing.TB, s claimcheck.Store) func() {
			f := fakes[s]
			f.fail(http.StatusServiceUnavailable, "SlowDown")
			return f.recover
		},
	})
}

// TestWhatS3SaidDecidesTheAnswer is the classification, one S3 answer at a
// time. Only NoSuchKey is a definite absence. AccessDenied and NoSuchBucket
// are 4xx answers that LOOK permanent and are configuration: dead-lettering
// on them would empty a topic into its dead-letter queue for a typo, and S3
// answers a missing key with AccessDenied whenever s3:ListBucket is not
// granted.
func TestWhatS3SaidDecidesTheAnswer(t *testing.T) {
	for _, c := range []struct {
		status int
		code   string
		want   error
	}{
		{http.StatusNotFound, "NoSuchKey", claimcheck.ErrPayloadGone},
		{http.StatusForbidden, "AccessDenied", claimcheck.ErrStoreUnavailable},
		{http.StatusNotFound, "NoSuchBucket", claimcheck.ErrStoreUnavailable},
		{http.StatusServiceUnavailable, "SlowDown", claimcheck.ErrStoreUnavailable},
		{http.StatusInternalServerError, "InternalError", claimcheck.ErrStoreUnavailable},
		{http.StatusBadRequest, "InvalidRequest", claimcheck.ErrStoreUnavailable},
	} {
		t.Run(c.code, func(t *testing.T) {
			f := newFakeS3(t)
			s := newStore(t, f.client())
			key, err := s.Put(context.Background(), []byte("payload"))
			if err != nil {
				t.Fatal(err)
			}
			f.fail(c.status, c.code)
			got, err := s.Get(context.Background(), key, 1<<20)
			if !errors.Is(err, c.want) || got != nil {
				t.Fatalf("Get when S3 answers %d %s = (%q, %v), want %v", c.status, c.code, got, err, c.want)
			}
			if errors.Is(c.want, claimcheck.ErrPayloadGone) {
				if errors.Is(err, claimcheck.ErrStoreUnavailable) {
					t.Fatal("NoSuchKey also reads as ErrStoreUnavailable")
				}
				return
			}
			if errors.Is(err, claimcheck.ErrPayloadGone) {
				t.Fatalf("%s reads as ErrPayloadGone: a good message would be dead-lettered", c.code)
			}
			if mq.CodeOf(err) != mq.CodeUnavailable {
				t.Errorf("code = %v, want CodeUnavailable so the runner retries", mq.CodeOf(err))
			}
			var api smithy.APIError
			if !errors.As(err, &api) || api.ErrorCode() != c.code {
				t.Errorf("the SDK's error is not reachable through the store's: %v", err)
			}
		})
	}
	t.Run("connection refused", func(t *testing.T) {
		f := newFakeS3(t)
		url := f.srv.URL
		f.srv.Close()
		s := newStore(t, newClient(url, "iqmq", "iqmq-secret"))
		_, err := s.Get(context.Background(), prefix+strings.Repeat("a", 32), 1<<20)
		if !errors.Is(err, claimcheck.ErrStoreUnavailable) || errors.Is(err, claimcheck.ErrPayloadGone) {
			t.Fatalf("Get from a server that is not there = %v, want ErrStoreUnavailable", err)
		}
	})
	t.Run("cancelled context", func(t *testing.T) {
		f := newFakeS3(t)
		s := newStore(t, f.client())
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := s.Get(ctx, prefix+strings.Repeat("a", 32), 1<<20)
		if !errors.Is(err, claimcheck.ErrStoreUnavailable) || !errors.Is(err, context.Canceled) {
			t.Fatalf("Get with a cancelled context = %v, want ErrStoreUnavailable carrying context.Canceled", err)
		}
	})
}

// TestAForgedKeyMakesNoRequest: a key the store could not have issued is
// ErrPayloadGone without a request, so a forged reference cannot read another
// object out of the bucket into a handler.
func TestAForgedKeyMakesNoRequest(t *testing.T) {
	f := newFakeS3(t)
	s := newStore(t, f.client())
	f.objectsForTest("config/secrets.json", []byte("somebody else's object"))
	before := f.requests.Load()
	for _, key := range []string{
		"config/secrets.json", "../config/secrets.json", prefix + "../config/secrets.json",
		prefix + strings.Repeat("a", 31), prefix + strings.Repeat("A", 32), strings.Repeat("a", 32),
		prefix + strings.Repeat("a", 32) + "/", "", prefix,
	} {
		if got, err := s.Get(context.Background(), key, 1<<20); !errors.Is(err, claimcheck.ErrPayloadGone) || got != nil {
			t.Errorf("Get(%q) = (%q, %v), want ErrPayloadGone", key, got, err)
		}
	}
	if n := f.requests.Load() - before; n != 0 {
		t.Fatalf("forged keys made %d request(s) to the bucket", n)
	}
}

// TestAnObjectOverTheLimitIsRefused: an object over the limit is ErrTooLarge.
// With a Content-Length the store refuses before reading the body; the
// refusal is the same either way, which is why the next test exists.
func TestAnObjectOverTheLimitIsRefused(t *testing.T) {
	f := newFakeS3(t)
	s := newStore(t, f.client())
	key, err := s.Put(context.Background(), bytes.Repeat([]byte("z"), 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(context.Background(), key, 1000); !errors.Is(err, claimcheck.ErrTooLarge) || got != nil {
		t.Fatalf("Get of a 1 MiB object with max 1000 = (%d bytes, %v), want ErrTooLarge", len(got), err)
	}
}

// TestKeysAreThePrefixAndThirtyTwoHex: what the store writes is what it will
// later agree to read, and nothing else.
func TestKeysAreThePrefixAndThirtyTwoHex(t *testing.T) {
	f := newFakeS3(t)
	s := newStore(t, f.client())
	key, err := s.Put(context.Background(), []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(key, prefix) || len(key) != len(prefix)+32 || strings.Trim(key[len(prefix):], "0123456789abcdef") != "" {
		t.Fatalf("Put issued key %q, want %q followed by 32 lower-case hex characters", key, prefix)
	}
	if keys := f.keys(); len(keys) != 1 || keys[0] != bucket+"/"+key {
		t.Fatalf("the bucket holds %v, want exactly [%s/%s]", keys, bucket, key)
	}
	if s.Name() != "s3://"+bucket+"/"+prefix {
		t.Errorf("Name() = %q", s.Name())
	}
}

func TestNewRefusals(t *testing.T) {
	c := newFakeS3(t).client()
	for name, opt := range map[string]s3store.Options{
		"no client":       {Bucket: bucket},
		"no bucket":       {Client: c},
		"name too long":   {Client: c, Bucket: bucket, Name: strings.Repeat("n", claimcheck.MaxStoreLen+1)},
		"prefix too long": {Client: c, Bucket: bucket, Prefix: strings.Repeat("p", claimcheck.MaxKeyLen)},
	} {
		if _, err := s3store.New(opt); !errors.Is(err, claimcheck.ErrInvalidConfig) {
			t.Errorf("%s: New = %v, want ErrInvalidConfig", name, err)
		}
	}
}

// TestCloseLeavesTheClientAlone: the client is lent, so a store built on it
// after another was closed still works.
func TestCloseLeavesTheClientAlone(t *testing.T) {
	f := newFakeS3(t)
	c := f.client()
	first := newStore(t, c)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := newStore(t, c)
	if _, err := second.Put(context.Background(), []byte("x")); err != nil {
		t.Fatalf("a store on a client another store had closed: %v", err)
	}
}

// TestAllocsAForgedKey: refusing a forged key costs nothing but the check.
func TestAllocsAForgedKey(t *testing.T) {
	s := newStore(t, newFakeS3(t).client())
	ctx := context.Background()
	var err error
	if got := testing.AllocsPerRun(200, func() {
		_, err = s.Get(ctx, "../config/secrets.json", 1<<20)
	}); got != 0 {
		t.Errorf("Get of a forged key allocs/op = %v, want 0", got)
	}
	if !errors.Is(err, claimcheck.ErrPayloadGone) {
		t.Fatal(err)
	}
}

// minioEnv names a MinIO the MinIO arm runs against, as host:port. It follows
// conformance/harness's IQMQ_<BROKER>_ADDR convention, and it is opt-in: `make
// ci` runs on a shared host and starts no container of its own.
const (
	minioAddrEnv     = "IQMQ_MINIO_ADDR"
	minioUserEnv     = "IQMQ_MINIO_USER"
	minioPasswordEnv = "IQMQ_MINIO_PASSWORD"
)

func minioClient(tb testing.TB) *s3.Client {
	tb.Helper()
	addr := os.Getenv(minioAddrEnv)
	if addr == "" {
		tb.Skipf("%s is unset; the MinIO arm runs against %s (conformance/harness/images.lock)", minioAddrEnv, pinnedMinIO)
	}
	user, password := os.Getenv(minioUserEnv), os.Getenv(minioPasswordEnv)
	if user == "" {
		user, password = "minioadmin", "minioadmin"
	}
	c := newClient("http://"+addr, user, password)
	_, err := c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	var owned *s3types.BucketAlreadyOwnedByYou
	if err != nil && !errors.As(err, &owned) {
		tb.Fatalf("CreateBucket on MinIO at %s: %v", addr, err)
	}
	return c
}

// TestContractAgainstMinIO is the same contract against a real S3-compatible
// server, where NoSuchKey, Content-Length and the key rules are S3's own and
// not the fake's. There is no Break here -- the test does not stop a server it
// did not start -- and the outage half is TestWhatS3SaidDecidesTheAnswer's.
func TestContractAgainstMinIO(t *testing.T) {
	c := minioClient(t)
	storetest.Run(t, storetest.Harness{
		New: func(tb testing.TB) claimcheck.Store { return newStore(tb, c) },
		Remove: func(tb testing.TB, _ claimcheck.Store, key string) {
			if _, err := c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
				tb.Fatal(err)
			}
		},
	})
}

// TestAMissingBucketIsUnavailableOnMinIO: a real server's answer for a bucket
// that does not exist is NoSuchBucket, a 404 -- and it must read as an outage
// to retry, not as a gone payload to dead-letter, or a mistyped bucket name
// empties a topic into its dead-letter queue.
func TestAMissingBucketIsUnavailableOnMinIO(t *testing.T) {
	c := minioClient(t)
	s, err := s3store.New(s3store.Options{Client: c, Bucket: "no-such-bucket-here", Prefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Get(context.Background(), prefix+strings.Repeat("a", 32), 1<<20)
	if !errors.Is(err, claimcheck.ErrStoreUnavailable) || errors.Is(err, claimcheck.ErrPayloadGone) {
		t.Fatalf("Get from a bucket that does not exist = %v, want ErrStoreUnavailable", err)
	}
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "NoSuchBucket" {
		t.Fatalf("MinIO's answer was not NoSuchBucket, so this test is not testing what it says: %v", err)
	}
	if _, err := s.Put(context.Background(), []byte("x")); !errors.Is(err, claimcheck.ErrStoreUnavailable) {
		t.Fatalf("Put to a bucket that does not exist = %v, want ErrStoreUnavailable", err)
	}
}

// TestAnOversizedBodyCostsNoMoreThanTheLimit: a body with NO declared length
// -- chunked, as a proxy or a streaming server may send it -- gives the store
// nothing to refuse on up front, so the read itself has to stop at the limit.
// A 64 MiB object fetched with a 64 KiB limit must not cost 64 MiB of memory:
// that is the decompression-bomb lesson applied to a body from a store.
func TestAnOversizedBodyCostsNoMoreThanTheLimit(t *testing.T) {
	f := newFakeS3(t)
	s := newStore(t, f.client())
	key, err := s.Put(context.Background(), bytes.Repeat([]byte("z"), 64<<20))
	if err != nil {
		t.Fatal(err)
	}
	f.chunked.Store(true)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	got, err := s.Get(context.Background(), key, 64<<10)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, claimcheck.ErrTooLarge) || got != nil {
		t.Fatalf("Get of a chunked 64 MiB body with a 64 KiB limit = (%d bytes, %v), want ErrTooLarge", len(got), err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 8<<20 {
		t.Fatalf("refusing a 64 MiB body with a 64 KiB limit allocated %d bytes: the read was not bounded by the limit", allocated)
	}
}
