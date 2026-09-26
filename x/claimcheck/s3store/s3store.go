package s3store

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/iqhive/iqmq/x/claimcheck"
)

// keyLen is the length of the random part of every key: 16 random bytes,
// hex-encoded.
const keyLen = 32

// Options configures a [Store].
type Options struct {
	// Client is the S3 client. Required, and LENT: the store never closes
	// it and never changes its configuration.
	Client *s3.Client
	// Bucket holds the payloads. Required.
	Bucket string
	// Prefix is prepended to every key the store issues, and a key without
	// it is refused. Optional; a prefix of its own ("claims/") lets the
	// bucket's lifecycle rule and its IAM policy name exactly these objects.
	Prefix string
	// Name is the store's identity, recorded in every reference. Empty takes
	// "s3://" + Bucket + "/" + Prefix. It must be the same on every
	// publisher and consumer of a topic; see [claimcheck.Store].
	Name string
}

// Store is a [claimcheck.Store] over one bucket and prefix. It is safe for
// concurrent use.
type Store struct {
	client *s3.Client
	bucket string
	prefix string
	name   string

	mu     sync.RWMutex
	closed bool
}

// New returns a store, or [claimcheck.ErrInvalidConfig] for a missing client
// or bucket, or a name or prefix too long for a reference.
func New(opt Options) (*Store, error) {
	if opt.Client == nil || opt.Bucket == "" {
		return nil, claimcheck.ErrInvalidConfig
	}
	if opt.Name == "" {
		opt.Name = "s3://" + opt.Bucket + "/" + opt.Prefix
	}
	if len(opt.Name) > claimcheck.MaxStoreLen || len(opt.Prefix)+keyLen > claimcheck.MaxKeyLen {
		return nil, claimcheck.ErrInvalidConfig
	}
	return &Store{client: opt.Client, bucket: opt.Bucket, prefix: opt.Prefix, name: opt.Name}, nil
}

// Name implements [claimcheck.Store].
func (s *Store) Name() string { return s.name }

func (s *Store) isClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

// Put implements [claimcheck.Store]. The payload is uploaded from a copy-free
// reader over the caller's slice, which the SDK has finished with when Put
// returns, so the caller may reuse its buffer afterwards.
func (s *Store) Put(ctx context.Context, payload []byte) (string, error) {
	if s.isClosed() {
		return "", claimcheck.ErrClosed
	}
	key := s.prefix + newKey()
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(len(payload))),
		ContentType:   aws.String("application/octet-stream"),
	})
	if err != nil {
		// Every failure to store is a failure to publish, and the
		// publication fails closed either way; ErrStoreUnavailable is the
		// class, and the SDK's error is joined for the caller.
		return "", errors.Join(claimcheck.ErrStoreUnavailable, err)
	}
	return key, nil
}

// Get implements [claimcheck.Store].
func (s *Store) Get(ctx context.Context, key string, maxBytes int64) ([]byte, error) {
	if !s.validKey(key) {
		return nil, claimcheck.ErrPayloadGone
	}
	if s.isClosed() {
		return nil, claimcheck.ErrClosed
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, classify(err)
	}
	defer func() { _ = out.Body.Close() }()
	size := aws.ToInt64(out.ContentLength)
	if size > maxBytes {
		return nil, claimcheck.ErrTooLarge
	}
	// The declared length sizes the buffer only once it is known to be
	// within the limit, and the read is bounded again by the limit: the
	// length is the server's claim, and a body longer than it is refused
	// rather than trusted.
	buf := bytes.NewBuffer(make([]byte, 0, max(size, 0)))
	n, err := io.Copy(buf, io.LimitReader(out.Body, maxBytes+1))
	if err != nil {
		return nil, errors.Join(claimcheck.ErrStoreUnavailable, err)
	}
	if n > maxBytes {
		return nil, claimcheck.ErrTooLarge
	}
	return buf.Bytes(), nil
}

// Close implements [claimcheck.Store]. It is idempotent, and it does not
// close the client, which was lent.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// classify turns an SDK error from GetObject into one of the contract's
// answers. Only NoSuchKey is a definite absence; see the package
// documentation for why AccessDenied and NoSuchBucket are not.
func classify(err error) error {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return claimcheck.ErrPayloadGone
	}
	var api smithy.APIError
	if errors.As(err, &api) && api.ErrorCode() == "NoSuchKey" {
		return claimcheck.ErrPayloadGone
	}
	return errors.Join(claimcheck.ErrStoreUnavailable, err)
}

// validKey reports whether key is one this store could have issued: its
// prefix, then exactly 32 lower-case hex characters.
func (s *Store) validKey(key string) bool {
	if len(key) != len(s.prefix)+keyLen || key[:len(s.prefix)] != s.prefix {
		return false
	}
	for i := len(s.prefix); i < len(key); i++ {
		c := key[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// newKey returns 16 bytes from crypto/rand, hex-encoded.
func newKey() string {
	var b [keyLen / 2]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error (Go 1.24+)
	return hex.EncodeToString(b[:])
}
