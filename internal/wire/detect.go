package wire

import (
	"sync"

	"github.com/iqhive/iqmq/internal/payload"
)

// probeLimits are DetectFull's limits: every bound removed and ZeroCopy set.
//
// The bounds are removed on purpose. DetectFull answers "is this an envelope",
// and that question must not depend on what any particular consumer is willing
// to accept: an envelope a producer wrote under a larger MaxBody is still an
// envelope, and a consumer that read it as a native body because it was too
// large would deliver the envelope's own bytes as a payload — a silent
// corruption in place of the refusal the limit exists to produce.
//
// ZeroCopy is set because the probe's payload is discarded: aliasing the input
// costs no copy, and DetectFull drops every aliased field before the payload
// goes back to the pool.
var probeLimits = Limits{
	MaxKey:         -1,
	MaxBody:        -1,
	MaxID:          -1,
	MaxHeaders:     -1,
	MaxHeaderKey:   -1,
	MaxHeaderValue: -1,
	MaxProp:        -1,
	ZeroCopy:       true,
}

// probeLayouts are the layouts DetectFull decodes through. They are package
// level because a Layout carries a 256-slot intern table: New allocates two
// kilobytes, which is not a per-probe cost worth paying.
var probeLayouts = [kindCount]Layout{Fixed: &fixedLayout{}, TLV: &tlvLayout{}}

// probePool holds DetectFull's scratch payloads.
var probePool = sync.Pool{New: func() any { return new(payload.Payload) }}

// DetectFull reports the layout of src when src is, exactly and entirely, one
// canonical envelope of a known layout — the question [Detect] approximates
// from six bytes.
//
// Detect reads the magic, the layout/version byte and the flags byte, so it
// answers true for any byte string that starts with those six bytes. That is
// the right answer for a caller choosing a decoder for something it already
// knows to be an envelope, and the wrong one for a caller deciding WHETHER a
// body is an envelope: a native publication whose body happens to begin with
// the magic is claimed, and on a broker with no place to record the answer out
// of band — NSQ has none — that turned a payload into either a corrupt
// delivery or a record that could be neither decoded nor settled for ever.
//
// DetectFull answers from the whole envelope instead: the framing, every
// declared length, the canonical field order and the absence of trailing
// bytes, which is the decoder's own arithmetic rather than a second reading of
// it. A body has to BE a canonical envelope to be claimed, not merely to start
// like one.
//
// It is not free — it is a decode into a pooled payload, discarded — so a
// caller that is going to decode anyway should decode first and use DetectFull
// only to tell a refused envelope from a native body (see
// [github.com/iqhive/iqmq/driverkit.Envelope.Unwrap]). It is still not a proof
// of provenance: a producer that deliberately writes a valid envelope as a
// native body is indistinguishable from one that enveloped it, which is why the
// only complete answer is a marker outside the body and why a driver that HAS
// one must use it.
func DetectFull(src []byte) (Kind, bool) {
	k, ok := Detect(src)
	if !ok {
		return 0, false
	}
	p, _ := probePool.Get().(*payload.Payload)
	err := probeLayouts[k].Decode(src, p, &probeLimits)
	// Every byte field aliases src under ZeroCopy, so the scratch drops them
	// before it is reused; the header slice keeps its capacity.
	p.Key, p.Body, p.ID = nil, nil, ""
	p.Props = payload.Props{}
	clear(p.Headers)
	p.Headers = p.Headers[:0]
	probePool.Put(p)
	if err != nil {
		return 0, false
	}
	return k, true
}
