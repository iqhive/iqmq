// Package s3store is a [claimcheck.Store] over any S3-compatible object store:
// Amazon S3, MinIO, Ceph RGW, Cloudflare R2, Google Cloud Storage's XML API.
//
// It is its own module because the AWS SDK is a dependency the root module may
// not have (ADR-002); x/claimcheck defines the Store interface, the reference
// and the two stdlib stores, and this module links the SDK.
//
// # Use
//
//	client := s3.NewFromConfig(cfg)                 // yours; the store never closes it
//	store, err := s3store.New(s3store.Options{
//	    Client: client, Bucket: "orders-payloads", Prefix: "claims/",
//	})
//	cc := claimcheck.Config{Store: store, RequireDigest: true}
//
// The client is LENT (the specification's ownership rule): the store never closes it and
// never changes its configuration, so credentials, region, endpoint, retries,
// TLS and checksum behaviour are all the caller's. Against an S3-compatible
// server that rejects the SDK's default request checksums, set the client's
// RequestChecksumCalculation to aws.RequestChecksumCalculationWhenRequired.
//
// # The three answers
//
// The Store contract has three failures and this is where they are decided,
// from what S3 said :
//
//   - **NoSuchKey** -- the bucket answered and the object is not there -- is
//     [claimcheck.ErrPayloadGone]: permanent, and x/claimcheck's CheckOut
//     dead-letters it on its first attempt.
//   - **Everything else** -- a timeout, a refused connection, a 5xx, a
//     throttle, a cancelled context, and also AccessDenied and NoSuchBucket --
//     is [claimcheck.ErrStoreUnavailable]: retryable, with the SDK's error
//     joined so errors.As still reaches it.
//   - A payload over the limit is [claimcheck.ErrTooLarge], decided from the
//     response's Content-Length before the body is read, and enforced again
//     while it is read. A Put that S3 refuses with **EntityTooLarge** (an
//     object over its single-PUT limit) is ErrTooLarge as well, because no
//     retry makes it fit; every other refusal of a Put is
//     ErrStoreUnavailable.
//
// AccessDenied and NoSuchBucket are the two that deserve a sentence each,
// because both are 4xx answers that look permanent. A missing bucket and a
// missing permission are CONFIGURATION: every message fails the same way
// until somebody fixes it, and dead-lettering them all as gone would empty the
// topic into the dead-letter queue for a typo. Retrying keeps them where a
// redeploy can reach them. And S3 answers GetObject on a missing key with 403
// AccessDenied, not 404 NoSuchKey, whenever the caller lacks s3:ListBucket on
// the bucket -- so a deployment without that permission cannot tell a gone
// payload from a forbidden one, and this store will retry, not dead-letter,
// its gone payloads. Grant s3:ListBucket on the payload bucket (or the prefix)
// to get the 404 the contract needs.
//
// # Keys
//
// The store names every object itself: Prefix followed by 32 random hex
// characters. A reference is remote input, so its key is: a key that is not
// in exactly that shape is ErrPayloadGone without a request being made. That
// is what stops a forged reference from reading another object out of the
// bucket -- a configuration file, another tenant's payload -- into a handler,
// which with DigestNone nothing else would notice.
//
// # Lifecycle
//
// Nothing here deletes. The consumer never deletes a payload (x/claimcheck's
// package documentation explains why), and deletion is the bucket's own
// lifecycle rule on the prefix, set longer than the broker's retention plus
// its longest redelivery window.
package s3store
