package redis

import (
	"context"
	"math"
	"strconv"
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/streams"
)

// The compile-time proof of what this backend is. Asserting it here is what
// makes a change to any of these a build failure rather than a surface that
// quietly stops being offered.
var (
	_ mq.Backend        = (*Backend)(nil)
	_ mq.HealthReporter = (*Backend)(nil)
	_ mq.AdminBackend   = (*Backend)(nil)
)

// Backend is the Redis Streams [github.com/iqhive/iqmq.Backend].
//
// It owns the client, the capability table, the error mapping and the health
// state machine, and it hands out sinks and sources over one client: rueidis
// pipelines everything on a small pool of connections, so a client per
// publisher would be spending file descriptors to no purpose.
type Backend struct {
	*driverkit.BaseBackend
	cfg     Config
	errs    *driverkit.ErrorMapper
	cmder   streams.Commander
	native  any
	caps    mq.Capabilities
	profile serverProfile
	clock   mq.Clock

	// alias is [streams.AliasesArgs] of cmder, asked once: whether a message
	// body may be sent as the caller's own []byte instead of copied into the
	// command. It is false for rueidis and true for go-redis, and the reason
	// is the client library's retention behaviour rather than a preference.
	alias bool

	// health is the reporter, held separately from BaseBackend's copy so
	// that a dial that fails can report Degraded before the base exists.
	health *driverkit.Health

	// mu guards the open children. A backend closes what it opened, which is the obligation and the reason a parked Fetch is woken rather than
	// left holding a goroutine after Close.
	mu     sync.Mutex
	srcs   map[*source]struct{}
	sinks  map[*sink]struct{}
	closed bool
}

// init finishes construction once a client exists.
func (b *Backend) init(ctx context.Context, cmder streams.Commander, native any, own driverkit.Ownership) error {
	scripts := make(map[string]string, len(streams.Scripts()))
	for _, script := range streams.Scripts() {
		scripts[streams.SHAOf(script)] = script
	}
	b.cmder, b.native = &reloader{inner: cmder, scripts: scripts}, native
	// the rule 4: a capability is compiled into a boolean at
	// construction, not asked per message. This one is also asked of the
	// WRAPPED commander deliberately -- the wrapper is what every command
	// goes through, so it is the thing whose retention behaviour matters.
	b.alias = streams.AliasesArgs(b.cmder)
	b.clock = b.cfg.Clock
	b.srcs, b.sinks = map[*source]struct{}{}, map[*sink]struct{}{}
	profile, err := b.probe(ctx)
	if err != nil {
		return err
	}
	if !profile.atLeast(MinVersionMajor, MinVersionMinor) {
		// The reclaim loop is XAUTOCLAIM, and without it nothing on this
		// broker redelivers. Refusing to open is the only honest answer:
		// opening would advertise at-least-once delivery on a server that
		// cannot provide it.
		//
		// But a version this driver could not PARSE is not the same as a
		// version that is too old, and refusing both would lock the driver
		// out of every managed Redis that hides INFO behind an ACL. So an
		// unparseable version falls back to asking the server directly
		// whether XAUTOCLAIM exists, which is the fact the floor is really
		// about.
		if profile.Major != 0 || profile.Minor != 0 {
			return errServerTooOld(profile.Version)
		}
		has, herr := b.probeXAutoClaim(ctx)
		if herr != nil {
			return herr
		}
		if !has {
			return errServerTooOld(profile.Version)
		}
	}
	b.profile = profile
	caps, err := capabilities(profile, b.cfg)
	if err != nil {
		return err
	}
	b.caps = caps
	if err := b.loadScripts(ctx); err != nil {
		return err
	}
	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Info:         b.info(profile),
		Capabilities: caps,
		Native:       native,
		Ownership:    own,
		CloseNative:  cmder.Close,
		Health:       b.health,
	}, b.teardown)
	if err != nil {
		return err
	}
	b.BaseBackend = base
	b.health.Ready()
	return nil
}

// probe asks the server what it is.
//
// Two things are read and both decide a capability claim: the version (WAITAOF
// needs 7.0, XAUTOCLAIM 6.2) and whether the server has an append-only file
// (WAITAOF's other condition). A driver that guessed either would advertise a
// durable boundary it cannot define.
func (b *Backend) probe(ctx context.Context) (serverProfile, error) {
	p := serverProfile{Cluster: len(b.cfg.Addrs) > 1}
	if info, ok := b.cmder.(streams.ServerInfo); ok {
		version, software, err := info.ServerVersion(ctx)
		if err != nil {
			return serverProfile{}, b.errs.Map(mq.OpOpen, "", "", err)
		}
		p.Version, p.Software = version, software
		p.Major, p.Minor = parseVersion(version)
	}
	p.AOF = b.probeAOF(ctx)
	return p, nil
}

// probeAOF reads `CONFIG GET appendonly`.
//
// It returns a bool and no error, deliberately. Every way this can fail — a
// managed Redis with CONFIG disabled, a restricted ACL, a reply shape the
// driver does not recognise — means the same thing: the driver does not know
// whether the server keeps an append-only file, and the specification forbids reading a
// fact it does not know as the favourable one. So "we do not know" is false,
// ConfirmDurable is refused at open rather than accepted and silently weaker
// than the caller asked for, and the capability table says the condition was
// not met. An error return would offer a caller a third answer there is
// nothing to do with.
func (b *Backend) probeAOF(ctx context.Context) bool {
	v, err := b.cmder.Do(ctx, streams.Cmd{Tokens: []string{"CONFIG", "GET", "appendonly"}})
	if err != nil {
		return false
	}
	items, ok := v.Arr()
	if !ok || len(items) < 2 {
		return false
	}
	val, ok := items[1].Str()
	if !ok {
		return false
	}
	return val == "yes"
}

// probeXAutoClaim asks the server whether it has XAUTOCLAIM.
//
// The probe is XAUTOCLAIM against a key that does not exist, which is
// harmless: a server that has the command answers NOGROUP (no such key or
// consumer group) and one that does not answers "unknown command". Nothing is
// created, nothing is claimed, and the question is answered by the server
// rather than by a version string it may not be willing to give.
func (b *Backend) probeXAutoClaim(ctx context.Context) (bool, error) {
	cmd, err := streams.XAutoClaim(streams.XAutoClaimArgs{
		Key:      "{iqmq-xautoclaim-probe}",
		Group:    "iqmq-probe",
		Consumer: "iqmq-probe",
		Start:    streams.IDZero,
		Count:    1,
	})
	if err != nil {
		return false, err
	}
	_, err = b.cmder.Do(ctx, cmd)
	switch {
	case err == nil:
		return true, nil
	case streams.IsNoGroup(err):
		return true, nil
	}
	return false, nil
}

// loadScripts sends every script body once, so that the hot paths use
// EVALSHA.
//
// A failure here is not fatal: the scripts are also sent by digest with a
// NOSCRIPT recovery, so a server that refuses SCRIPT LOAD (a restricted ACL)
// still works, one round trip more expensive the first time each script is
// used. On a cluster SCRIPT LOAD names no key and reaches one node, so the
// recovery is also what fills the other nodes' caches.
func (b *Backend) loadScripts(ctx context.Context) error {
	for _, script := range streams.Scripts() {
		if _, err := b.cmder.Do(ctx, streams.ScriptLoad(script)); err != nil {
			// Nothing to do about it, and nothing broken by it.
			b.health.Degraded(b.errs.Map(mq.OpOpen, "", "", err))
			b.health.Ready()
			return nil
		}
	}
	return nil
}

// reloader is the [streams.Commander] every path in this driver goes through:
// the inner commander plus a recovery from an empty script cache.
//
// NOSCRIPT is the one server error this driver handles itself, and it has to
// be handled HERE rather than on one route, because every settlement route is
// an EVALSHA: the guarded acknowledgement, the guarded extension, the
// dead-letter move, the delayed park, the delayed release and the re-add. A
// server that restarted, failed over or ran SCRIPT FLUSH has an empty cache,
// and a driver that noticed on only some of those routes would stop being
// able to acknowledge anything after a failover while still being able to
// publish — which is the shape of an outage nobody can diagnose.
//
// The recovery is to send the command again as EVAL, with the script's body
// in place of its digest. EVAL carries the command's own keys, so a cluster
// routes it to the node whose cache was empty, and that node caches the
// script as it runs it. The first recovery here was a SCRIPT LOAD followed
// by the same EVALSHA, and on a cluster that does not work: SCRIPT LOAD names
// no key, so the client sends it to whichever node it likes, and the node
// that answered NOSCRIPT stays empty and answers it again. It is bounded to
// one retry, and EVAL cannot answer NOSCRIPT, so nothing can spin.
type reloader struct {
	inner   streams.Commander
	scripts map[string]string
}

var (
	_ streams.Commander = (*reloader)(nil)
	_ pinner            = (*reloader)(nil)
)

// Do sends one command, recovering from an empty script cache.
func (r *reloader) Do(ctx context.Context, cmd streams.Cmd) (streams.Value, error) {
	v, err := r.inner.Do(ctx, cmd)
	if err == nil || !streams.IsNoScript(err) {
		return v, err
	}
	eval, ok := r.asEval(cmd)
	if !ok {
		return v, err
	}
	return r.inner.Do(ctx, eval)
}

// DoMulti pipelines cmds, retrying only the commands that met an empty cache.
//
// Only those, and the reason is not economy: a pipeline's other commands
// SUCCEEDED, and re-sending them would re-send an XADD — which on this broker
// is a second copy of the message, not an idempotent retry.
func (r *reloader) DoMulti(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	results, err := r.inner.DoMulti(ctx, cmds)
	if err != nil {
		return results, err
	}
	for i := range results {
		if i >= len(cmds) || results[i].Err == nil || !streams.IsNoScript(results[i].Err) {
			continue
		}
		eval, ok := r.asEval(cmds[i])
		if !ok {
			continue
		}
		v, rerr := r.inner.Do(ctx, eval)
		results[i] = streams.Result{Value: v, Err: rerr}
	}
	return results, nil
}

// DoPinned delegates when the inner commander can pin a connection, and is
// DoMulti otherwise.
//
// The delegation is what lets a pinned call reach the client at all: the sink
// asks this wrapper, so one that did not forward the surface would send every
// durable publication's WAITAOF as unpinned as before. It recovers no script
// cache, because what it carries is a publication's XADDs and their WAITAOF.
func (r *reloader) DoPinned(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	if p, ok := r.inner.(pinner); ok {
		return p.DoPinned(ctx, cmds)
	}
	return r.DoMulti(ctx, cmds)
}

// Close closes the inner commander.
func (r *reloader) Close() error { return r.inner.Close() }

// Nodes delegates when the inner commander can address each node of a
// cluster, and is nil otherwise. A wrapper that did not forward it would make
// ListTopics send one SCAN cursor to whichever node the client picked.
func (r *reloader) Nodes(ctx context.Context) ([]streams.Commander, error) {
	if nl, ok := r.inner.(nodeLister); ok {
		return nl.Nodes(ctx)
	}
	return nil, nil
}

// Ping delegates when the inner commander has a cheap liveness check.
func (r *reloader) Ping(ctx context.Context) error {
	if p, ok := r.inner.(streams.Pinger); ok {
		return p.Ping(ctx)
	}
	_, err := r.inner.Do(ctx, streams.Ping())
	return err
}

// ServerVersion delegates when the inner commander can answer.
func (r *reloader) ServerVersion(ctx context.Context) (string, string, error) {
	if info, ok := r.inner.(streams.ServerInfo); ok {
		return info.ServerVersion(ctx)
	}
	return "", "", nil
}

// AliasArgs delegates when the inner commander accepts blob tokens.
//
// The delegation matters for the same reason ClientLibrary's does and fails
// more quietly: a wrapper that did not forward it would make every commander
// in the repository answer "copies", so the go-redis driver would silently go
// on paying the body copy with every test still green. The reverse — a
// wrapper that claimed more than its inner commander — is the dangerous
// direction, and it cannot happen here: the answer is the inner commander's.
//
// This wrapper itself is safe to forward from. Its own extra work is the
// script reload, which re-sends the SAME command inside Do after loading the
// script the command named by digest, so the retry happens before Do returns
// and the caller has not got its buffer back yet.
func (r *reloader) AliasArgs() bool {
	return streams.AliasesArgs(r.inner)
}

// ClientLibrary delegates when the inner commander names a client library.
//
// The delegation is not optional politeness: this wrapper is what `info()`
// asks, so a reloader that did not forward the surface would answer "no client
// library" for every commander in the repository.
func (r *reloader) ClientLibrary() (module, version string) {
	if info, ok := r.inner.(streams.ClientLibrary); ok {
		return info.ClientLibrary()
	}
	return "", ""
}

// asEval is an EVALSHA command rewritten as the EVAL of the script it names
// by digest, with the same keys and arguments in the same positions.
//
// The tokens are copied: the caller's command may be retried or inspected
// after this returns, and its tokens are not this wrapper's to edit.
func (r *reloader) asEval(cmd streams.Cmd) (streams.Cmd, bool) {
	if cmd.Name() != "EVALSHA" || len(cmd.Tokens) < 2 {
		return streams.Cmd{}, false
	}
	script, ok := r.scripts[cmd.Tokens[1]]
	if !ok {
		return streams.Cmd{}, false
	}
	out := cmd
	out.Tokens = append([]string(nil), cmd.Tokens...)
	out.Tokens[0], out.Tokens[1] = "EVAL", script
	return out, true
}

// info describes the driver and the library behind it. Every field the
// driver does not know is empty, which the specification makes "the driver did not say"
// rather than a claim.
func (b *Backend) info(p serverProfile) mq.DriverInfo {
	profile := p.Software
	if profile == "" {
		profile = "redis"
	}
	if p.Cluster {
		profile += "-cluster"
	}

	var clientName, clientVersion string
	if lib, ok := b.cmder.(streams.ClientLibrary); ok {
		clientName, clientVersion = lib.ClientLibrary()
	}
	return mq.DriverInfo{
		Name:          DriverName,
		Profile:       profile,
		DriverVersion: Version,
		ClientName:    clientName,
		ClientVersion: clientVersion,
		BrokerVersion: p.Version,
	}
}

// Ping is the liveness check one round trip that consumes no message
// and creates no topology.
func (b *Backend) Ping(ctx context.Context) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	if p, ok := b.cmder.(streams.Pinger); ok {
		if err := p.Ping(ctx); err != nil {
			return b.errs.MapNet(mq.OpOpen, "", "", err, false)
		}
		return nil
	}
	if _, err := b.cmder.Do(ctx, streams.Ping()); err != nil {
		return b.errs.MapNet(mq.OpOpen, "", "", err, false)
	}
	return nil
}

// Profile returns what the driver learned about the server, which is what the
// capability table branched on.
//
// It is exported because the version matrix needs it: a matrix run asserts
// that the claims really do flip between 6.2, 7.4, 8 and Valkey, and it can
// only do that if it can see which server it got.
func (b *Backend) Profile() (version, software string, aof, cluster bool) {
	return b.profile.Version, b.profile.Software, b.profile.AOF, b.profile.Cluster
}

// Commander returns the command layer this backend sends through.
//
// It is the escape hatch under a name a caller can find, and it is the seam
// `drivers/redis/goredis` replaces: everything above it is
// `internal/streams`, so a second client library is a second [Commander] and
// nothing else.
func (b *Backend) Commander() streams.Commander { return b.cmder }

// OpenSink opens a publication channel to one topic.
//
// ConfirmDurable is refused HERE and not later, which is the rule: a
// publisher that discovered at its first message that this server has no
// durable boundary would already have told the application it was safe to
// forget the data.
func (b *Backend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	durable := cfg.Confirmation.Resolve() == mq.ConfirmDurable
	if durable && !b.canConfirmDurable() {
		return nil, errNoDurableConfirm
	}
	if cfg.Receipts {
		// FeatureReceipts is not advertised, so the core should never ask;
		// refusing rather than ignoring is what keeps a future core change
		// from silently handing callers an unfilled receipt.
		return nil, errNoReceipts
	}
	meta, err := metaOf(cfg.Metadata)
	if err != nil {
		return nil, err
	}
	keys, err := b.cfg.keysFor(cfg.Topic)
	if err != nil {
		return nil, err
	}
	env, err := b.envelope(cfg.Metadata)
	if err != nil {
		return nil, err
	}
	if !b.cfg.AutoCreate {
		if err := b.requireStreams(ctx, keys); err != nil {
			return nil, err
		}
	}
	s := &sink{
		b:       b,
		topic:   cfg.Topic,
		keys:    keys,
		meta:    meta,
		env:     env,
		durable: durable,
		alias:   b.alias,
		closed:  make(chan struct{}),
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, mq.ErrClosed
	}
	b.sinks[s] = struct{}{}
	return s, nil
}

// canConfirmDurable reports whether this server can define a durable
// boundary: WAITAOF exists (7.0) and there is an append-only file to wait on.
func (b *Backend) canConfirmDurable() bool {
	return b.profile.atLeast(7, 0) && b.profile.AOF
}

// requireStreams refuses a topic whose streams do not exist, which is the specification's
// rule with AutoCreate off: nothing creates topology behind an operator's
// back.
func (b *Backend) requireStreams(ctx context.Context, keys streams.Keys) error {
	cmds := make([]streams.Cmd, 0, keys.Shards)
	for i := range keys.Shards {
		cmds = append(cmds, streams.Exists(keys.Stream(i)))
	}
	results, err := b.cmder.DoMulti(ctx, cmds)
	if err != nil {
		return b.errs.Map(mq.OpOpen, keys.Topic, "", err)
	}
	for _, res := range results {
		if res.Err != nil {
			return b.errs.Map(mq.OpOpen, keys.Topic, "", res.Err)
		}
		n, cerr := streams.ParseCount(res.Value)
		if cerr != nil {
			return b.errs.Map(mq.OpOpen, keys.Topic, "", cerr)
		}
		if n == 0 {
			return errNoAutoCreate
		}
	}
	return nil
}

// OpenSource opens a subscription.
//
// The subscription name becomes the consumer group, which is what makes two
// consumers of one subscription compete and two consumers of different
// subscriptions each receive everything.
func (b *Backend) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	if cfg.Prefetch <= 0 {
		return nil, errUnresolvedPrefetch
	}
	meta, err := metaOf(cfg.Metadata)
	if err != nil {
		return nil, err
	}
	keys, err := b.cfg.keysFor(cfg.Topic)
	if err != nil {
		return nil, err
	}
	group, err := streams.Group(cfg.Subscription)
	if err != nil {
		return nil, err
	}
	start, err := startOf(cfg.Start)
	if err != nil {
		return nil, err
	}
	src, err := b.newSource(ctx, cfg, keys, group, start, meta)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		// Close takes no context: the specification gives it none, because a prompt
		// close is not an operation a caller cancels and Shutdown is the
		// bounded form. There is nothing to pass down.
		_ = src.Close() //nolint:contextcheck // the specification gives Close no context; see above
		return nil, mq.ErrClosed
	}
	b.srcs[src] = struct{}{}
	b.mu.Unlock()
	return src, nil
}

// startOf maps the start positions onto Redis Streams.
//
// Three of the five map exactly and two need a word of explanation.
// StartEarliest is "0-0", the beginning of whatever the stream still holds —
// which is not "every message ever published", because a trimmed stream has
// forgotten the rest, and the driver page says so. StartAt(time) works because
// a stream identifier's high half IS the publication time in milliseconds, so
// "the first entry at or after t" is a real position rather than an
// approximation; the resolution is one millisecond.
//
// XGROUP CREATE takes the identifier of the last entry the group has seen, not
// the first one to deliver, so StartAt is the last identifier of the
// millisecond before t's. It used to be "<t-ms>-0", which is the identifier
// Redis gives the first entry of t's millisecond, and that entry was skipped.
func startOf(pos mq.StartPosition) (string, error) {
	switch pos.Resolve().Kind() {
	case mq.StartKindLatest:
		return streams.IDLast, nil
	case mq.StartKindEarliest:
		return streams.IDZero, nil
	case mq.StartKindTime:
		t := pos.Time()
		if t.IsZero() {
			return streams.IDZero, nil
		}
		ms := t.UnixMilli()
		if ms <= 0 {
			// Every entry is after "0-0", so the group delivers all of them.
			return streams.IDZero, nil
		}
		return streams.ID{Ms: uint64(ms) - 1, Seq: math.MaxUint64}.String(), nil
	case mq.StartKindSequence:
		// A sequence is a PACKED stream identifier, which is what
		// DeliveryInfo.Sequence carried out. Packing both halves rather than
		// reporting the millisecond alone is what makes this resume exact:
		// a resume from a millisecond would replay every entry published in
		// it, which on a busy stream is thousands of duplicates.
		return streams.Unpack(pos.Sequence()).String(), nil
	case mq.StartKindUnset:
		return streams.IDLast, nil
	}
	return "", errBadStartPosition
}

// envelope builds the metadata shim for one handle.
//
// The limits are handed to BOTH halves, which is the whole point: Wrap
// encodes anything and Unwrap decodes under wire.Limits, so a limit set given
// only to the decoder accepts a message it can never read back. See
// [streams.Limits] and.
func (b *Backend) envelope(mode mq.MetadataMode) (*driverkit.Envelope, error) {
	lim := envelopeLimits
	return driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:         mode,
		Limits:       &lim,
		Capabilities: b.caps,
	})
}

// forget drops a closed child.
func (b *Backend) forget(src *source, s *sink) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if src != nil {
		delete(b.srcs, src)
	}
	if s != nil {
		delete(b.sinks, s)
	}
}

// teardown closes everything this backend opened, before the client goes.
//
// It is what wakes a Fetch that is parked in a source nobody closed: the specification's
// "Close unblocks blocked operations" has to reach a caller who closed the
// client rather than the consumer.
func (b *Backend) teardown() error {
	b.mu.Lock()
	b.closed = true
	srcs := make([]*source, 0, len(b.srcs))
	for s := range b.srcs {
		srcs = append(srcs, s)
	}
	sinks := make([]*sink, 0, len(b.sinks))
	for s := range b.sinks {
		sinks = append(sinks, s)
	}
	b.mu.Unlock()
	var first error
	for _, s := range srcs {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	for _, s := range sinks {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// now reads the injected clock.
func (b *Backend) now() time.Time {
	if b.clock != nil {
		return b.clock.Now()
	}
	return time.Now()
}

// metadataMode is what this driver does with a message's metadata.
type metadataMode uint8

const (
	// metaStrict maps every field to a stream entry field and refuses
	// anything that would not come back byte for byte.
	metaStrict metadataMode = iota
	// metaDrop leaves out what it cannot carry.
	metaDrop
	// metaEnvelope puts the whole message in the portable binary envelope.
	metaEnvelope
	// metaAdaptive uses the native mapping where it suffices and the
	// envelope where it does not.
	metaAdaptive
)

// metaOf maps the four modes onto this driver's.
//
// Three of the four behave identically HERE, and saying so is more useful than
// pretending otherwise: a stream entry is an ordered list of binary-safe
// field-value pairs, so the entry layout carries the whole promise and
// there is no metadata this broker cannot represent. Strict never refuses,
// lenient never drops, and adaptive never envelopes. Only
// MetadataPortableEnvelope differs, and it differs because the CALLER asked
// for a wire format rather than because the driver needed one.
func metaOf(m mq.MetadataMode) (metadataMode, error) {
	switch m {
	case mq.MetadataNativeStrict:
		return metaStrict, nil
	case mq.MetadataLenientDrop:
		return metaDrop, nil
	case mq.MetadataPortableEnvelope:
		return metaEnvelope, nil
	case mq.MetadataAdaptive:
		return metaAdaptive, nil
	default:
		return metaStrict, errMetadataMode
	}
}

// errServerTooOld names the version this driver refuses to open against.
func errServerTooOld(version string) error {
	if version == "" {
		version = "unknown"
	}
	return &tooOldError{version: version}
}

// tooOldError is the refusal of a server below [MinVersionMajor].[MinVersionMinor].
type tooOldError struct{ version string }

func (e *tooOldError) Error() string {
	return "iqmq/redis: this server reports version " + e.version +
		", and the mandatory pending-entries reclaim loop needs XAUTOCLAIM (Redis " +
		strconv.Itoa(MinVersionMajor) + "." + strconv.Itoa(MinVersionMinor) +
		"). Without it nothing on this broker redelivers, so at-least-once delivery would be advertised and absent"
}

func (e *tooOldError) Unwrap() error { return mq.ErrUnsupported }

// ClientModule is the client library this driver wraps, and rueidisVersion its
// version. They are held here so that the backend does not import the library
// for one constant, and ClientModule is exported so that a caller choosing
// between the two Redis drivers can name the one it got.
const (
	ClientModule   = "github.com/redis/rueidis"
	rueidisVersion = "v1.0.79"
)
