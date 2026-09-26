package streams_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/internal/streams/fakestreams"
	"github.com/iqhive/iqmq/wire"
)

// The target is not "encode then decode agrees", which is the easy half. It is
// the pair of promises that together make a publication safe:
//
//  1. If [streams.Limits.Check] accepts a message, [streams.Encode] followed
//     by [streams.Decode] reproduces it exactly — including the nil-versus-
//     empty distinction of key and body, the order of duplicate header keys,
//     and binary values.
//  2. If Check accepts a message, the portable envelope can carry it too:
//     [driverkit.Envelope] in MetadataPortableEnvelope mode wraps and unwraps
//     it under the same limits. That is the hole — Wrap encodes anything and
//     Unwrap decodes under wire.Limits — and the mode is one a caller can ask
//     for on this driver, so a message admitted above those bounds is a record
//     published successfully and then undecodable for ever.
//
// A message Check REFUSES proves nothing about either, which is why the
// refusal is asserted separately rather than skipped: a Check that refused
// everything would satisfy both promises and be useless.
func FuzzRedisFields(f *testing.F) {
	lim := streams.DefaultLimits
	envelope, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:   mq.MetadataPortableEnvelope,
		Limits: envelopeLimits(),
	})
	if err != nil {
		f.Fatalf("NewEnvelope: %v", err)
	}
	seedFields(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		m := fuzzMessage(r)
		if cerr := lim.Check(m); cerr != nil {
			// The refusal is the other half of the contract: whatever Check
			// refuses, it must refuse for a reason the limits name, and it
			// must not then be silently encodable-and-undecodable. Nothing
			// more is asserted about it — the driver never publishes it.
			if !errors.Is(cerr, mq.ErrMessageTooLarge) && !errors.Is(cerr, mq.ErrInvalidTopic) {
				t.Fatalf("Check refused a message with %v, which is neither MessageTooLarge nor InvalidArgument", cerr)
			}
			return
		}
		// Promise 1: the entry field layout.
		fields := streams.Encode(nil, m)
		if len(fields)%2 != 0 {
			t.Fatalf("Encode produced %d fields, which is not name/value pairs", len(fields))
		}
		var back mq.Message
		if derr := streams.Decode(&back, fields); derr != nil {
			t.Fatalf("Decode of an accepted message: %v", derr)
		}
		if diff := sameMessage(m, &back); diff != "" {
			t.Fatalf("the field layout did not round-trip an accepted message: %s", diff)
		}
		// Encoding is canonical: two encodings of one message are the same
		// bytes, which is what lets a differential test compare entries.
		again := streams.Encode(nil, m)
		if len(again) != len(fields) {
			t.Fatalf("Encode is not deterministic: %d fields then %d", len(fields), len(again))
		}
		for i := range fields {
			if fields[i] != again[i] {
				t.Fatalf("Encode is not deterministic at field %d", i)
			}
		}
		// Promise 1b: the aliased encoding is the SAME entry. It skips the
		// body's copy, so a message whose body is the one field the two
		// encoders treat differently is exactly what this target already
		// generates -- and a divergence here is an entry that decodes
		// differently depending on which client library published it.
		af, ab := streams.EncodeAliased(nil, nil, m)
		aliased := streams.XAdd(streams.XAddArgs{Key: "{t}:0", ID: streams.IDAuto, Fields: af, FieldBlobs: ab})
		if !aliased.Valid() {
			t.Fatalf("XAdd built an invalid command from EncodeAliased: tokens %q blobs %v",
				aliased.Tokens, aliased.Blobs)
		}
		plain := streams.XAdd(streams.XAddArgs{Key: "{t}:0", ID: streams.IDAuto, Fields: fields})
		mat := aliased.Materialize()
		if len(mat.Tokens) != len(plain.Tokens) {
			t.Fatalf("the aliased command materialised to %d tokens, the copied one has %d",
				len(mat.Tokens), len(plain.Tokens))
		}
		for i := range mat.Tokens {
			if mat.Tokens[i] != plain.Tokens[i] {
				t.Fatalf("token %d differs between the aliased and the copied encoding", i)
			}
			if aliased.Text(i) != plain.Tokens[i] {
				t.Fatalf("Text(%d) differs between the aliased and the copied encoding", i)
			}
		}
		var aliasedBack mq.Message
		if derr := streams.Decode(&aliasedBack, mat.Tokens[len(mat.Tokens)-len(af):]); derr != nil {
			t.Fatalf("Decode of the aliased entry: %v", derr)
		}
		if diff := sameMessage(m, &aliasedBack); diff != "" {
			t.Fatalf("the aliased field layout did not round-trip an accepted message: %s", diff)
		}
		// Promise 2: the portable envelope carries it too.
		wrapped, werr := envelope.Wrap(m)
		if werr != nil {
			t.Fatalf("the portable envelope refused a message Check accepted: %v", werr)
		}
		var unwrapped mq.Message
		if uerr := envelope.Unwrap(&unwrapped, wrapped); uerr != nil {
			t.Fatalf("the portable envelope encoded a message it cannot decode: %v — "+
				"that publication would be accepted and then break every Fetch on the "+
				"subscription for ever ", uerr)
		}
		unwrapped.Topic = m.Topic
		if diff := sameMessage(m, &unwrapped); diff != "" {
			t.Fatalf("the portable envelope did not round-trip an accepted message: %s", diff)
		}
		// And the enveloped message goes through the field layout as well,
		// which is the shape a MetadataPortableEnvelope publication really
		// takes: the envelope's bytes become the entry's body.
		envFields := streams.Encode(nil, wrapped)
		var envBack mq.Message
		if derr := streams.Decode(&envBack, envFields); derr != nil {
			t.Fatalf("Decode of an enveloped message's entry: %v", derr)
		}
		var final mq.Message
		if uerr := envelope.Unwrap(&final, &envBack); uerr != nil {
			t.Fatalf("the enveloped entry did not decode after a field-layout round trip: %v", uerr)
		}
	})
}

// envelopeLimits is the limit set handed to BOTH halves of the envelope.
//
// Handing it to the decoder only is the mistake the specification found: the default on the
// encode side is no limit at all, so a message over the bound is accepted and
// then undecodable.
func envelopeLimits() *wire.Limits {
	lim := wire.DefaultLimits
	return &lim
}

// fuzzMessage builds a message from the fuzzer's bytes, deliberately reaching
// past the limits: the sizes are drawn around the bounds rather than below
// them, because the interesting inputs are the ones one byte over.
func fuzzMessage(r *fuzzutil.Reader) *mq.Message {
	m := &mq.Message{}
	switch r.Choice(3) {
	case 0:
		m.Body = nil
	case 1:
		m.Body = []byte{}
	default:
		m.Body = r.Bytes(512)
	}
	switch r.Choice(3) {
	case 0:
		m.Key = nil
	case 1:
		m.Key = []byte{}
	default:
		m.Key = r.Bytes(300)
	}
	m.Topic = r.String(64)
	m.ID = r.String(300)
	for n := r.Int(0, 6); n > 0; n-- {
		m.Headers.Add(r.String(300), r.Bytes(300))
	}
	if r.Bool() {
		m.Properties.ContentType = r.String(64)
	}
	if r.Bool() {
		m.Properties.ContentEncoding = r.String(64)
	}
	if r.Bool() {
		m.Properties.CorrelationID = r.String(64)
	}
	if r.Bool() {
		m.Properties.CausationID = r.String(64)
	}
	if r.Bool() {
		m.Properties.ReplyTo = r.String(64)
	}
	if r.Bool() {
		m.Properties.CreatedAt = time.Unix(int64(r.Uint64()>>2)-(1<<40), int64(r.Uint32()%1e9)).UTC()
	}
	return m
}

// seedFields commits the inputs that matter: the two the specification found against the
// toy driver's envelope, translated into this layout, plus the framing corners
// of the header packing.
func seedFields(f *testing.F) {
	seeds := []func(*fuzzutil.Writer){
		// A body, no key, one ordinary header.
		func(w *fuzzutil.Writer) {
			w.Choice(3, 2)
			w.Bytes(512, []byte("payload"))
			w.Choice(3, 0)
			w.String(64, "acme.orders")
			w.String(300, "")
			w.Int(0, 6, 1)
			w.String(300, "trace")
			w.Bytes(300, []byte("abc"))
			for range 6 {
				w.Bool(false)
			}
		},
		// A header key one byte over wire.DefaultLimits.MaxHeaderKey: the
		// input the fuzzer found in five seconds.
		func(w *fuzzutil.Writer) {
			w.Choice(3, 2)
			w.Bytes(512, []byte("x"))
			w.Choice(3, 0)
			w.String(64, "t")
			w.String(300, "")
			w.Int(0, 6, 1)
			w.String(300, repeat('k', wire.DefaultLimits.MaxHeaderKey+1))
			w.Bytes(300, []byte("v"))
			for range 6 {
				w.Bool(false)
			}
		},
		// A message identifier one byte over MaxID: the input it found in
		// the next sixty-seven seconds, against a fix that bounded only the
		// headers.
		func(w *fuzzutil.Writer) {
			w.Choice(3, 2)
			w.Bytes(512, []byte("x"))
			w.Choice(3, 0)
			w.String(64, "t")
			w.String(300, repeat('i', wire.DefaultLimits.MaxID+1))
			w.Int(0, 6, 0)
			for range 6 {
				w.Bool(false)
			}
		},
		// A header key containing the framing's own separator, an empty
		// header key, and a binary value: the three inputs that break a
		// separator-based packing.
		func(w *fuzzutil.Writer) {
			w.Choice(3, 2)
			w.Bytes(512, []byte("x"))
			w.Choice(3, 1)
			w.String(64, "t")
			w.String(300, "")
			w.Int(0, 6, 3)
			w.String(300, "9:not-a-length")
			w.Bytes(300, []byte{0, ':', 0xff})
			w.String(300, "")
			w.Bytes(300, nil)
			w.String(300, "dup")
			w.Bytes(300, []byte("1"))
			for range 6 {
				w.Bool(false)
			}
		},
		// Every property set, with a CreatedAt outside the range a single
		// unix-nanosecond count can hold.
		func(w *fuzzutil.Writer) {
			w.Choice(3, 2)
			w.Bytes(512, []byte("x"))
			w.Choice(3, 0)
			w.String(64, "t")
			w.String(300, "id")
			w.Int(0, 6, 0)
			for range 5 {
				w.Bool(true)
				w.String(64, "p")
			}
			w.Bool(true)
			w.Uint64(1 << 55)
			w.Uint32(123456789)
		},
	}
	for _, seed := range seeds {
		w := fuzzutil.NewWriter()
		seed(w)
		f.Add(w.Data())
	}
}

func repeat(c byte, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = c
	}
	return string(b)
}

// reclaimState is the op-script state of [FuzzRedisReclaimer]: one fake
// server, two consumers of one group, and a model of what the broker owes.
type reclaimState struct {
	srv    *fakestreams.Server
	keys   streams.Keys
	a, b   *streams.Consumer
	held   map[string][]streams.Delivery
	latest map[string]map[streams.EntryKey]uint32
	acked  map[streams.EntryKey]string
	ctx    context.Context
	ids    []streams.ID
	seq    int
}

// The op script drives two consumers of one group against one fake server,
// interleaving reads, reclaim sweeps, acknowledgements, nacks, lease
// extensions and clock jumps in whatever order the fuzzer picks. The
// invariant is the one that decides whether a message is lost:
//
//	an entry whose pending row names consumer X must not be removed from the
//	pending list by consumer Y's acknowledgement.
//
// The model is the fake's own pending list, which is why the fake's XACK
// deliberately does NOT check ownership: the check has to be the driver's, and
// a fake that made it for the driver would let a driver with no guard at all
// pass this target.
//
// The second invariant is the mirror image: a delivery the reclaimer still
// says is ours must be acknowledgeable. A driver that refused everything would
// satisfy the first invariant and deliver nothing.
func FuzzRedisReclaimer(f *testing.F) {
	script := fuzzutil.Script[*reclaimState]{
		Steps: []fuzzutil.Step[*reclaimState]{
			{Name: "publish", Weight: 4, Run: func(t testing.TB, r *fuzzutil.Reader, s *reclaimState) {
				s.publish(t, r.Int(1, 3))
			}},
			{Name: "read-a", Weight: 4, Run: func(t testing.TB, _ *fuzzutil.Reader, s *reclaimState) {
				s.read(t, "a")
			}},
			{Name: "read-b", Weight: 4, Run: func(t testing.TB, _ *fuzzutil.Reader, s *reclaimState) {
				s.read(t, "b")
			}},
			{Name: "sweep-a", Weight: 3, Run: func(t testing.TB, _ *fuzzutil.Reader, s *reclaimState) {
				s.sweep(t, "a")
			}},
			{Name: "sweep-b", Weight: 3, Run: func(t testing.TB, _ *fuzzutil.Reader, s *reclaimState) {
				s.sweep(t, "b")
			}},
			{Name: "ack-a", Weight: 4, Run: func(t testing.TB, r *fuzzutil.Reader, s *reclaimState) {
				s.ack(t, "a", r)
			}},
			{Name: "ack-b", Weight: 4, Run: func(t testing.TB, r *fuzzutil.Reader, s *reclaimState) {
				s.ack(t, "b", r)
			}},
			{Name: "nack-a", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, s *reclaimState) {
				s.nack(t, "a", r)
			}},
			{Name: "extend-a", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, s *reclaimState) {
				s.extend(t, "a", r)
			}},
			{Name: "advance", Weight: 3, Run: func(t testing.TB, r *fuzzutil.Reader, s *reclaimState) {
				s.srv.Advance(r.Duration(3 * testLease))
			}},
		},
	}
	seedReclaimer(f, &script)
	f.Fuzz(func(t *testing.T, data []byte) {
		s := newReclaimState(t)
		script.Run(t, data, s)
		s.checkFinal(t)
	})
}

func newReclaimState(t testing.TB) *reclaimState {
	t.Helper()
	keys, err := streams.NewKeys(testTopic, 2)
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	srv := fakestreams.New(fakestreams.Options{AOF: true})
	s := &reclaimState{
		srv:    srv,
		keys:   keys,
		held:   map[string][]streams.Delivery{},
		latest: map[string]map[streams.EntryKey]uint32{"a": {}, "b": {}},
		acked:  map[streams.EntryKey]string{},
		ctx:    context.Background(),
	}
	for _, name := range []string{"a", "b"} {
		cons, cerr := streams.NewConsumer(streams.ConsumerConfig{
			Keys:        keys,
			Group:       testGroup,
			Name:        "source-" + name,
			Prefetch:    8,
			AckDeadline: testLease,
		})
		if cerr != nil {
			t.Fatalf("NewConsumer: %v", cerr)
		}
		if name == "a" {
			s.a = cons
		} else {
			s.b = cons
		}
	}
	if err := s.a.Provision(s.ctx, srv, streams.IDZero, true); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	return s
}

func (s *reclaimState) cons(who string) *streams.Consumer {
	if who == "a" {
		return s.a
	}
	return s.b
}

func (s *reclaimState) name(who string) string { return "source-" + who }

func (s *reclaimState) publish(t testing.TB, n int) {
	t.Helper()
	for range n {
		s.seq++
		shard := s.seq % s.keys.Shards
		m := &mq.Message{Topic: testTopic, Body: []byte{byte(s.seq)}}
		v, err := s.srv.Do(s.ctx, streams.XAdd(streams.XAddArgs{
			Key:    s.keys.Stream(shard),
			ID:     streams.IDAuto,
			Fields: streams.Encode(nil, m),
		}))
		if err != nil {
			t.Fatalf("XADD: %v", err)
		}
		id, err := streams.ParseXAdd(v)
		if err != nil {
			t.Fatalf("ParseXAdd: %v", err)
		}
		s.ids = append(s.ids, id)
	}
}

func (s *reclaimState) read(t testing.TB, who string) {
	t.Helper()
	ds, _, err := s.cons(who).Read(s.ctx, s.srv, nil, nil)
	if err != nil {
		t.Fatalf("%s Read: %v", who, err)
	}
	s.record(who, ds)
}

func (s *reclaimState) sweep(t testing.TB, who string) {
	t.Helper()
	ds, _, err := s.cons(who).Sweep(s.ctx, s.srv, nil, nil)
	if err != nil {
		t.Fatalf("%s Sweep: %v", who, err)
	}
	s.record(who, ds)
}

// record remembers a consumer's deliveries and the latest generation it was
// handed for each entry.
//
// The latest generation is the model's half of the supersede rule: a
// settlement carrying it must be accepted, and one carrying anything older
// must be refused. Without it the target could not tell a guard that works
// from a guard that refuses everything.
func (s *reclaimState) record(who string, ds []streams.Delivery) {
	s.held[who] = append(s.held[who], ds...)
	for _, d := range ds {
		s.latest[who][d.Key] = d.Gen
	}
}

// ack settles some of one consumer's held deliveries and checks the
// invariant: the outcome must agree with who the server says owns the entry.
func (s *reclaimState) ack(t testing.TB, who string, r *fuzzutil.Reader) {
	t.Helper()
	held := s.held[who]
	if len(held) == 0 {
		return
	}
	n := r.Int(1, len(held))
	batch := held[:n]
	s.held[who] = held[n:]

	// Who the server says holds each entry, sampled BEFORE the
	// acknowledgement. It is the model: the guard's answer has to agree
	// with it.
	owners := s.owners()
	errs := s.cons(who).Ack(s.ctx, s.srv, batch)
	if len(errs) != len(batch) {
		t.Fatalf("Ack returned %d results for %d deliveries; a result not aligned with the batch "+
			"reports one delivery's outcome for another", len(errs), len(batch))
	}
	after := s.owners()
	// An entry can leave the pending list because ANOTHER item of the same
	// batch settled it legitimately: a batch may hold two generations of one
	// entry, the superseded one refused and the current one acknowledged.
	// Without this the target reports the refusal as the cause of a removal
	// the acceptance performed.
	settledHere := map[streams.EntryKey]bool{}
	for i, d := range batch {
		if errs[i] == nil {
			settledHere[d.Key] = true
		}
	}
	for i, d := range batch {
		owner, pending := owners[d.Key]
		switch {
		case errs[i] == nil:
			// It claimed to settle. The entry must have been ours, and it
			// must be gone from the pending list.
			if pending && owner != s.name(who) {
				t.Fatalf("%s acknowledged entry %v, which the server said was held by %q: "+
					"that removes the pending entry the other consumer is processing, and if that "+
					"consumer dies the message is lost ",
					who, d.Key, owner)
			}
			if _, still := after[d.Key]; still {
				t.Fatalf("%s reported entry %v acknowledged and it is still pending", who, d.Key)
			}
			s.acked[d.Key] = who
		case errors.Is(errs[i], mq.ErrLeaseLost):
			if settledHere[d.Key] {
				// A superseded generation refused beside its successor's
				// acceptance: the refusal is exactly right and the removal
				// is the successor's.
				continue
			}
			// It refused. The entry must NOT have been removed.
			if newOwner, still := after[d.Key]; pending && !still {
				t.Fatalf("%s refused to acknowledge entry %v with ErrLeaseLost and the entry left "+
					"the pending list anyway (owner was %q, now %q)", who, d.Key, owner, newOwner)
			}
			if pending && owner == s.name(who) && d.Gen == s.latest[who][d.Key] {
				// Refusing something we do hold under its CURRENT generation
				// is the failure mode a "refuse everything" guard has: the
				// subscription becomes unsettleable and the target would
				// otherwise call that a pass.
				t.Fatalf("%s refused to acknowledge entry %v, which the server says it holds "+
					"under the generation it was last handed (%d): a guard that refuses a live "+
					"delivery makes the subscription unsettleable", who, d.Key, d.Gen)
			}
		default:
			t.Fatalf("%s Ack of %v: %v", who, d.Key, errs[i])
		}
	}
}

func (s *reclaimState) nack(t testing.TB, who string, r *fuzzutil.Reader) {
	t.Helper()
	held := s.held[who]
	if len(held) == 0 {
		return
	}
	i := r.Choice(len(held))
	d := held[i]
	s.held[who] = append(held[:i:i], held[i+1:]...)
	delete(s.latest[who], d.Key)
	err := s.cons(who).Nack(s.ctx, s.srv, d, mq.NackOptions{}, s.srv.Now())
	switch {
	case err == nil, errors.Is(err, mq.ErrLeaseLost), errors.Is(err, mq.ErrUnsupported):
	default:
		t.Fatalf("%s Nack of %v: %v", who, d.Key, err)
	}
}

func (s *reclaimState) extend(t testing.TB, who string, r *fuzzutil.Reader) {
	t.Helper()
	held := s.held[who]
	if len(held) == 0 {
		return
	}
	n := r.Int(1, len(held))
	owners := s.owners()
	errs := s.cons(who).Extend(s.ctx, s.srv, held[:n])
	after := s.owners()
	for i, d := range held[:n] {
		if errs[i] != nil {
			if !errors.Is(errs[i], mq.ErrLeaseLost) {
				t.Fatalf("%s Extend of %v: %v", who, d.Key, errs[i])
			}
			continue
		}
		// An extension that succeeded must not have taken the entry from
		// somebody else: XCLAIM with min-idle 0 would do exactly that, which
		// is why the reclaimer's check comes first.
		if owner, pending := owners[d.Key]; pending && owner != s.name(who) {
			if after[d.Key] == s.name(who) {
				t.Fatalf("%s extended entry %v and thereby STOLE it from %q; an extension is not "+
					"a claim", who, d.Key, owner)
			}
		}
	}
}

// owners maps each pending entry to the consumer the server says holds it.
func (s *reclaimState) owners() map[streams.EntryKey]string {
	out := map[streams.EntryKey]string{}
	for shard := range s.keys.Shards {
		for _, p := range s.srv.Pending(s.keys.Stream(shard), testGroup) {
			out[streams.EntryKey{Shard: shard, ID: p.ID}] = p.Consumer
		}
	}
	return out
}

// checkFinal is the end-state invariant: no message was both acknowledged and
// still pending, and nothing was acknowledged twice by two different
// consumers.
func (s *reclaimState) checkFinal(t testing.TB) {
	t.Helper()
	owners := s.owners()
	for k, who := range s.acked {
		if owner, pending := owners[k]; pending {
			t.Fatalf("entry %v was acknowledged by %s and is pending again under %q with no "+
				"redelivery in between", k, who, owner)
		}
	}
	// Every entry that is neither acknowledged nor pending has left the
	// group, and under the DEFAULT strategy with no dead-letter destination
	// nothing takes an entry out of the group except an acknowledgement — a
	// nack leaves it pending. So an entry that is in neither set is a message
	// the driver lost, and the count is asserted rather than walked.
	lost := 0
	for _, id := range s.ids {
		for shard := range s.keys.Shards {
			k := streams.EntryKey{Shard: shard, ID: id}
			if _, pending := owners[k]; pending {
				continue
			}
			if _, done := s.acked[k]; done {
				continue
			}
			// The entry may be in another shard: an identifier is unique per
			// shard, so only one of the shards holds it.
			if s.everSeen(k) {
				lost++
			}
		}
	}
	if lost > 0 {
		t.Fatalf("%d entr(y/ies) are neither pending nor acknowledged: under the leave-pending "+
			"strategy with no dead-letter destination, nothing takes an entry out of the group "+
			"except an acknowledgement", lost)
	}
}

// everSeen reports whether this run ever handed the entry out, which is how
// the final check tells "this shard never held that identifier" from "the
// entry is gone".
func (s *reclaimState) everSeen(k streams.EntryKey) bool {
	if _, ok := s.latest["a"][k]; ok {
		return true
	}
	if _, ok := s.latest["b"][k]; ok {
		return true
	}
	return false
}

// seedReclaimer commits the interleavings that matter: the cross-consumer
// steal, the self-supersede, and a settlement racing a sweep.
func seedReclaimer(f *testing.F, script *fuzzutil.Script[*reclaimState]) {
	seeds := [][]string{
		// B claims A's entry, then A acknowledges it: the loss.
		{"publish", "read-a", "advance", "sweep-b", "ack-a"},
		// A's own sweep supersedes A's delivery, then A acknowledges the
		// old one and then the new one.
		{"publish", "read-a", "advance", "sweep-a", "ack-a", "ack-a"},
		// A nack leaves the entry pending, the sweep brings it back, and B
		// takes it in between.
		{"publish", "read-a", "nack-a", "advance", "sweep-b", "sweep-a", "ack-b"},
		// An extension against an entry somebody else holds.
		{"publish", "read-a", "advance", "sweep-b", "extend-a", "ack-b"},
		// Both consumers reading and acknowledging concurrently across two
		// shards.
		{"publish", "publish", "read-a", "read-b", "ack-a", "ack-b", "advance", "sweep-a", "ack-a"},
	}
	for _, steps := range seeds {
		w := fuzzutil.NewWriter()
		for _, name := range steps {
			script.WriteStep(w, name)
		}
		f.Add(w.Data())
	}
}
