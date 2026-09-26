package memory

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	mq "github.com/iqhive/iqmq"
)

// Rendering a configuration is an obligation rather than a convenience.
//
// There is no credential in a memory Config, so this closes a CONSISTENCY
// gap and not a leak, and the honest reason to close it is the check it makes
// possible: with every Config rendering itself, "every exported Config
// implements both" becomes a mechanical rule a tool can hold, and the next
// Config to carry a password inherits the rule instead of being audited by
// hand.

// DSN renders the configuration as a `memory://` connection string.
//
// It is a ROUND TRIP for every setting the DSN grammar carries: ParseDSN of
// this result returns a Config equal to the one it came from, which
// TestDSNRoundTripsEveryParameter holds against a Config with every such field
// set. That is the property worth having, because the alternative -- a
// rendering written beside a parser, each knowing a different set of parameter
// names -- is the exact defect class this repository keeps finding (the 259-byte header key, MaxHeaderKey of 259).
//
// The settings a DSN cannot carry -- a clock, hooks, a pinned variant kind,
// the scheduler resolution, the two refused per-lane bounds -- are NOT here.
// [Config.String] names them, and names them after a '#' so that the result
// cannot be mistaken for a DSN and pasted back: ParseDSN refuses a fragment.
func (c Config) DSN() string {
	var b strings.Builder
	b.WriteString(Scheme)
	b.WriteString("://")
	q := make([]string, 0, 20)
	if c.AutoCreate {
		q = append(q, "autocreate=true")
	}
	t := c.DefaultTopology
	if t.Kind != TopologyDefault {
		q = append(q, "kind="+t.Kind.String())
	}
	switch {
	case t.CapacityMessages < 0:
		q = append(q, "capacity=unbounded")
	case t.CapacityMessages > 0:
		q = append(q, "capacity="+strconv.Itoa(t.CapacityMessages))
	}
	if t.CapacityBytes > 0 {
		q = append(q, "capacitybytes="+strconv.FormatInt(t.CapacityBytes, 10))
	}
	switch {
	case t.Partitions < 0:
		q = append(q, "partitions=auto")
	case t.Partitions > 0:
		q = append(q, "partitions="+strconv.Itoa(t.Partitions))
	}
	if t.Ordering != OrderingDefault {
		q = append(q, "ordering="+t.Ordering.String())
	}
	if t.Overflow != OverflowDefault {
		q = append(q, "overflow="+t.Overflow.String())
	}
	if t.Fanout != FanoutAuto {
		q = append(q, "fanout="+t.Fanout.String())
	}
	switch {
	case t.AckDeadline < 0:
		q = append(q, "ackdeadline=never")
	case t.AckDeadline > 0:
		q = append(q, "ackdeadline="+t.AckDeadline.String())
	}
	if t.MaxAttempts > 0 {
		q = append(q, "maxattempts="+strconv.FormatUint(uint64(t.MaxAttempts), 10))
	}
	if t.DeadLetterTopic != "" {
		q = append(q, "deadletter="+t.DeadLetterTopic)
	}
	if t.MaxMessageSize > 0 {
		q = append(q, "maxmessagesize="+strconv.Itoa(t.MaxMessageSize))
	}
	if t.DedupWindow > 0 {
		q = append(q, "dedupwindow="+t.DedupWindow.String())
	}
	if t.Priority {
		q = append(q, "priority=true")
	}
	if t.Wildcards {
		q = append(q, "wildcards=true")
	}
	if t.Retention.MaxMessages > 0 {
		q = append(q, "retentionmessages="+strconv.Itoa(t.Retention.MaxMessages))
	}
	if t.Retention.MaxBytes > 0 {
		q = append(q, "retentionbytes="+strconv.FormatInt(t.Retention.MaxBytes, 10))
	}
	if t.Retention.MaxAge > 0 {
		q = append(q, "retentionage="+t.Retention.MaxAge.String())
	}
	if c.MaxTotalMessages > 0 {
		q = append(q, "maxtotalmessages="+strconv.Itoa(c.MaxTotalMessages))
	}
	if c.MaxTotalBytes > 0 {
		q = append(q, "maxtotalbytes="+strconv.FormatInt(c.MaxTotalBytes, 10))
	}
	if c.ZeroOnRelease {
		q = append(q, "zeroonrelease=true")
	}
	if c.Engine.Safe {
		q = append(q, "engine=safe")
	}
	if len(q) > 0 {
		b.WriteByte('?')
		b.WriteString(strings.Join(q, "&"))
	}
	return b.String()
}

// beyondDSN names the settings this configuration holds that no DSN can
// carry, in a fixed order, or nothing when it holds none.
//
// A function value is reported as set and never rendered: a hook's or a
// clock's identity is a pointer, and a pointer in a log line is noise a reader
// cannot act on. The two per-lane bounds are here because a non-zero value in
// either is REFUSED by resolve  -- a configuration that
// carries one is going to fail, and a rendering that hid it would make the
// refusal look like it came from nowhere.
func (c Config) beyondDSN() []string {
	out := make([]string, 0, 8)
	if c.SchedulerResolution > 0 {
		out = append(out, "schedulerresolution="+c.SchedulerResolution.String())
	}
	if c.Clock != nil {
		out = append(out, "clock=set")
	}
	if c.DebugOwnership {
		out = append(out, "debugownership=true")
	}
	if n := hookCount(c.Hooks); n > 0 {
		out = append(out, "hooks="+strconv.Itoa(n))
	}
	if c.DefaultTopology.MaxInFlight != 0 {
		out = append(out, "maxinflight="+strconv.Itoa(c.DefaultTopology.MaxInFlight))
	}
	if c.DefaultTopology.MaxDelayed != 0 {
		out = append(out, "maxdelayed="+strconv.Itoa(c.DefaultTopology.MaxDelayed))
	}
	if k := c.Engine.pinned(); k != "" {
		out = append(out, "engine."+k)
	}
	return out
}

// pinned names the variant families this selection pins away from Auto, or
// the empty string when every family is at its recorded default. Safe is not
// here: the DSN carries it.
func (e EngineConfig) pinned() string {
	pairs := [...]struct {
		name string
		slug string
		zero bool
	}{
		{"queue", e.Queue.String(), e.Queue == QueueAuto},
		{"arch", e.Arch.String(), e.Arch == ArchAuto},
		{"wait", e.Wait.String(), e.Wait == WaitAuto},
		{"timer", e.Timer.String(), e.Timer == TimerAuto},
		{"hash", e.Hash.String(), e.Hash == HashAuto},
		{"pool", e.Pool.String(), e.Pool == PoolAuto},
		{"registry", e.Registry.String(), e.Registry == RegistryAuto},
		{"stats", e.Stats.String(), e.Stats == StatsAuto},
		{"retrylane", e.RetryLane.String(), e.RetryLane == RetryLaneAuto},
	}
	out := make([]string, 0, len(pairs))
	for _, p := range pairs {
		if !p.zero {
			out = append(out, p.name+"="+p.slug)
		}
	}
	if len(out) == 0 {
		return ""
	}
	return strings.Join(out, ",")
}

// String renders the configuration (the security checks).
//
// It is [Config.DSN] plus, after a '#', the settings no DSN can carry. The
// fragment is what says "this is a rendering, not a connection string":
// ParseDSN refuses a fragment outright, so a reader who pastes the result back
// gets an error rather than a Config missing half its settings.
func (c Config) String() string {
	d := c.DSN()
	extra := c.beyondDSN()
	if len(extra) == 0 {
		return d
	}
	return d + "#" + strings.Join(extra, ",")
}

// LogValue renders the configuration for slog (the security checks).
func (c Config) LogValue() slog.Value { return slog.StringValue(c.String()) }

// hookCount counts the observation points a caller installed. mq.Hooks is a
// struct of five function fields with no method of its own, so the count is
// taken here; the specification names all five, so a sixth would be a compile error rather
// than a silently uncounted hook.
func hookCount(h mq.Hooks) int {
	n := 0
	for _, f := range [...]bool{
		h.OnPublishStart != nil,
		h.OnPublishDone != nil,
		h.OnReceive != nil,
		h.OnSettle != nil,
		h.OnReconnect != nil,
	} {
		if f {
			n++
		}
	}
	return n
}

// The two renderings, asserted here so that dropping one is a compile
// error in this package rather than a missing line in somebody's log.
var (
	_ fmt.Stringer   = Config{}
	_ slog.LogValuer = Config{}
)
