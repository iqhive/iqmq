package main

import (
	"sort"

	"github.com/iqhive/iqmq/driverkit/flush"
	"github.com/iqhive/iqmq/driverkit/nack"
	"github.com/iqhive/iqmq/driverkit/offset"
	"github.com/iqhive/iqmq/drivers/activemq"
	"github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/drivers/kafka"
	"github.com/iqhive/iqmq/drivers/kafka/kafkago"
	"github.com/iqhive/iqmq/drivers/nats"
	"github.com/iqhive/iqmq/drivers/nsq"
	"github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/rabbitmq"
	rabbitamqp10 "github.com/iqhive/iqmq/drivers/rabbitmq/amqp10"
	"github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/snssqs"
	"github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/backoff"
	"github.com/iqhive/iqmq/internal/engine"
	"github.com/iqhive/iqmq/internal/rabbit/confirm"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
	"github.com/iqhive/iqmq/internal/streams"
	"github.com/iqhive/iqmq/memory"
)

// driverEntry is one driver this binary can open a DSN with.
//
// The CLI links every shipped driver, so its module requires their client
// libraries and the root module's stdlib-only policy stops at this module's
// boundary. A CLI that linked only the stdlib-only drivers
// could open memory:// and nothing else, which is not a tool anybody can point
// at a broker.
type driverEntry struct {
	// name is the driver's own registered name.
	name string
	// module is the Go module that ships it, which is what a user adds to
	// their own go.mod.
	module string
	// schemes are the DSN schemes it answers to, canonical first.
	schemes []string
	// register registers it with a registry.
	register func(dsn.Registrar) error
	// describe parses a DSN into the driver's own typed configuration and
	// renders it through the driver's safe String, which the specification requires to
	// carry no credential. It is how `doctor` reports what a DSN resolved to
	// without performing any I/O, and it is the fourth place a credential can
	// appear.
	//
	// It is nil for a driver whose Config renders nothing safe — today only
	// the memory driver, whose configuration carries no credential at all.
	describe func(string) (string, error)
	// resolved reports the variant kinds in force for one DSN, read from the
	// driver's own typed configuration after it has parsed. It performs no
	// I/O: a kind is selected by the DSN or by the family's Auto binding,
	// and neither needs a broker.
	//
	// It is nil for a driver that exposes no variant family through its DSN,
	// which is most of them: the families a network driver selects are the
	// ones the specification gives it, and a driver with none has nothing to report.
	resolved func(string) ([]kindReport, error)
}

// kindRow builds one row of the per-target variant report.
//
// The Auto-versus-named distinction is the whole reason a row is not just a
// slug: "the DSN asked for sidequeue" and "the DSN said nothing and the
// bake-off's provisional winner happens to be sidequeue" are different facts,
// and only the second one changes when a campaign is re-run. fromDSN carries
// it, and the family's own provisional flag is looked up here so that a
// caller cannot forget to.
func kindRow(family, key, slug string, fromDSN bool) kindReport {
	source := "auto"
	if fromDSN {
		source = "dsn"
	}
	provisional := false
	if f, ok := familyByID(family); ok {
		provisional = f.provisional
	}
	return kindReport{Family: family, Key: key, Kind: slug, Source: source, Provisional: provisional}
}

// describeFunc adapts a driver's ParseDSN and its Config's String into the
// signature driverEntry.describe takes. The type parameter is the driver's own
// Config, so the rendering is the driver's own and never this package: the specification
// makes the safe String the configuration's obligation, and a CLI that
// reimplemented it would be a second, unproved redaction.
func describeFunc[C interface{ String() string }](parse func(string) (C, error)) func(string) (string, error) {
	return func(s string) (string, error) {
		cfg, err := parse(s)
		if err != nil {
			return "", err
		}
		return cfg.String(), nil
	}
}

// drivers is every driver this binary registers, in the order it registers
// them. The order is fixed so that a scheme conflict is a build-time property
// of this table rather than of an import graph.
func drivers() []driverEntry {
	return []driverEntry{
		{
			name: memory.DriverName, module: "github.com/iqhive/iqmq/memory",
			schemes:  []string{memory.Scheme, memory.SchemeAlias},
			register: memory.Register,
			// memory.Config implements neither String nor LogValue: it is
			// the one shipped configuration that carries no credential, so
			// there was nothing to redact. `doctor` reports the
			// resolved engine kinds for a memory target instead.
			describe: nil,
			resolved: memoryKinds,
		},
		{
			name: kafka.DriverName, module: "github.com/iqhive/iqmq/drivers/kafka",
			schemes: []string{kafka.Scheme}, register: kafka.Register,
			describe: describeFunc(kafka.ParseDSN), resolved: kafkaKinds,
		},
		{
			name: "kafkago", module: "github.com/iqhive/iqmq/drivers/kafka/kafkago",
			schemes: []string{kafkago.Scheme}, register: kafkago.Register,
			describe: describeFunc(kafkago.ParseDSN), resolved: kafkagoKinds,
		},
		{
			name: nats.DriverName, module: "github.com/iqhive/iqmq/drivers/nats",
			schemes: []string{nats.Scheme}, register: nats.Register,
			describe: describeFunc(nats.ParseDSN),
		},
		{
			name: jetstream.DriverName, module: "github.com/iqhive/iqmq/drivers/jetstream",
			schemes: []string{jetstream.Scheme}, register: jetstream.Register,
			describe: describeFunc(jetstream.ParseDSN),
		},
		{
			name: nsq.DriverName, module: "github.com/iqhive/iqmq/drivers/nsq",
			schemes: []string{nsq.Scheme}, register: nsq.Register,
			describe: describeFunc(nsq.ParseDSN),
		},
		{
			name: gcppubsub.DriverName, module: "github.com/iqhive/iqmq/drivers/gcppubsub",
			schemes: []string{gcppubsub.Scheme}, register: gcppubsub.Register,
			describe: describeFunc(gcppubsub.ParseDSN),
		},
		{
			name: sqs.DriverName, module: "github.com/iqhive/iqmq/drivers/sqs",
			schemes: []string{sqs.Scheme}, register: sqs.Register,
			describe: describeFunc(sqs.ParseDSN), resolved: sqsKinds,
		},
		{
			name: snssqs.DriverName, module: "github.com/iqhive/iqmq/drivers/snssqs",
			schemes: []string{snssqs.Scheme}, register: snssqs.Register,
			describe: describeFunc(snssqs.ParseDSN), resolved: snssqsKinds,
		},
		{
			name: rabbitmq.DriverName, module: "github.com/iqhive/iqmq/drivers/rabbitmq",
			schemes: []string{rabbitmq.Scheme}, register: rabbitmq.Register,
			describe: describeFunc(rabbitmq.ParseDSN), resolved: rabbitKinds,
		},
		{
			name: "rabbitmq+amqp10", module: "github.com/iqhive/iqmq/drivers/rabbitmq/amqp10",
			schemes: []string{rabbitamqp10.Scheme}, register: rabbitamqp10.Register,
			describe: describeFunc(rabbitamqp10.ParseDSN),
		},
		{
			name: activemq.DriverName, module: "github.com/iqhive/iqmq/drivers/activemq",
			schemes: []string{activemq.Scheme}, register: activemq.Register,
			describe: describeFunc(activemq.ParseDSN),
		},
		{
			name: "activemq+stomp", module: "github.com/iqhive/iqmq/drivers/activemq/stomp",
			schemes: []string{stomp.Scheme}, register: stomp.Register,
			describe: describeFunc(stomp.ParseDSN),
		},
		{
			name: redis.DriverName, module: "github.com/iqhive/iqmq/drivers/redis",
			schemes: []string{redis.Scheme}, register: redis.Register,
			describe: describeFunc(redis.ParseDSN), resolved: redisKinds,
		},
		{
			name: pulsar.DriverName, module: "github.com/iqhive/iqmq/drivers/pulsar",
			schemes: []string{pulsar.Scheme, pulsar.SchemeTLS}, register: pulsar.Register,
			describe: describeFunc(pulsar.ParseDSN),
		},
	}
}

// unreachable is a shipped driver module this binary does NOT register, and
// why. It is reported by `doctor --drivers` rather than left out, because "the
// CLI cannot open it" and "iqmq has no such driver" are different facts and a
// user comparing the CLI against the driver matrix will otherwise read the
// first as the second.
type unreachable struct {
	module string
	reason string
	// schemes are the DSN schemes the module claims, when it names any. It is
	// what lets a command answer "the scheme is claimed by a module this
	// binary cannot register" instead of "no driver claims that scheme": the
	// two read the same to a user and mean entirely different things, and
	// only the first says where to look.
	//
	// It is empty for a module that claims none -- gcppubsub/v1compat and
	// redis/goredis share another driver's scheme and export no Register at
	// all, so there is no scheme of theirs to name.
	schemes []string
}

// unreachableDrivers lists them. Every entry is a property of the shipped
// driver surface and not of this CLI.
func unreachableDrivers() []unreachable {
	return []unreachable{
		{
			module: "github.com/iqhive/iqmq/drivers/amqp10",
			reason: "the generic AMQP 1.0 core ships no Profile value, and its Register takes one; " +
				"a DSN scheme cannot be registered for it without a caller authoring the profile in Go",
			schemes: []string{amqp10.Scheme},
		},
		{
			module: "github.com/iqhive/iqmq/drivers/natsunified",
			reason: "its schemes are nats:// and natsjs://, which drivers/nats and drivers/jetstream " +
				"already claim; one registry cannot hold both, so the CLI registers the two profile " +
				"drivers, which is what lets doctor name the profile that answered",
			// Spelled from the two PROFILE drivers' own constants rather
			// than from natsunified's, which would make the CLI depend on
			// the module it cannot register. They are the same two strings
			// by construction: the collision this entry describes IS that
			// natsunified claims exactly what these two claim, so a change
			// to either constant moves both sides together.
			schemes: []string{nats.Scheme, jetstream.Scheme},
		},
		{
			module: "github.com/iqhive/iqmq/drivers/gcppubsub/v1compat",
			reason: "it exports no Register: it is the second client-library arm and shares " +
				"gcppubsub's DSN, so one scheme cannot select between them",
		},
		{
			module: "github.com/iqhive/iqmq/drivers/redis/goredis",
			reason: "it exports no Register: it is the second client-library arm and shares " +
				"drivers/redis's DSN and Config, so one scheme cannot select between them",
		},
	}
}

// newRegistry builds a registry holding every driver of [drivers].
//
// It is a registry of its own and not the process-wide one: dsn/global is a
// module a program opts into, and a CLI that owns its registry can be tested
// with a registry that holds one driver (ADR-012).
func newRegistry() (*dsn.Registry, error) {
	r := dsn.NewRegistry()
	for _, d := range drivers() {
		if err := d.register(r); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// entryForScheme returns the table entry that claims a scheme.
func entryForScheme(scheme string) (driverEntry, bool) {
	for _, d := range drivers() {
		for _, s := range d.schemes {
			if s == scheme {
				return d, true
			}
		}
	}
	return driverEntry{}, false
}

// schemes returns every scheme the table claims, sorted, for the help text.
func schemes() []string {
	all := drivers()
	out := make([]string, 0, 2*len(all))
	for _, d := range all {
		out = append(out, d.schemes...)
	}
	sort.Strings(out)
	return out
}

// The per-driver kind readers. Each one parses the DSN with the driver's own
// ParseDSN and reads the kind-typed fields of the Config it produced, which is
// the only place the answer exists: a kind is a typed field with no string
// parsing, so there is nothing to look up and nothing to guess.

// memoryKinds reports the engine's nine families for a memory target, plus the
// fan-out admission policy the DSN's topology carries.
//
// A memory DSN selects kinds only in bulk: `engine=safe` pins every family to
// its mutex-backed reference kind at once, and there is no per-family
// parameter. So the source is the same for all nine rows, which is why they
// are built in one loop rather than one at a time.
func memoryKinds(s string) ([]kindReport, error) {
	cfg, err := memory.ParseDSN(s)
	if err != nil {
		return nil, err
	}
	pinned := cfg.Engine.Safe
	r := cfg.Engine.Resolve()
	rows := make([]kindReport, 0, 10)
	rows = append(rows,
		kindRow("A1", "queue", r.Queue.String(), pinned),
		kindRow("A2", "arch", r.Arch.String(), pinned),
		kindRow("A3", "wait", r.Wait.String(), pinned),
		kindRow("A4", "timer", r.Timer.String(), pinned),
		kindRow("A5", "hash", r.Hash.String(), pinned),
		kindRow("A6", "pool", r.Pool.String(), pinned),
		kindRow("A9", "stats", r.Stats.String(), pinned),
		kindRow("A27", "registry", r.Registry.String(), pinned),
		kindRow("A28", "retry", r.RetryLane.String(), pinned),
	)
	// The specification is a per-topic admission policy rather than an engine kind, and the
	// memory DSN sets it with `fanout=`; its zero value is the family's Auto.
	// memory.FanoutPolicy has no Resolve of its own, so Auto is mapped here
	// through the family's own generated binding rather than by repeating the
	// winner's name.
	fan := cfg.DefaultTopology.Fanout
	fanSlug := fan.String()
	if fan == memory.FanoutAuto {
		fanSlug = engine.FanoutAutoKind.String()
	}
	rows = append(rows, kindRow("A7", "fanout", fanSlug, fan != memory.FanoutAuto))
	return rows, nil
}

// kafkaKinds reports families (offset tracker) and (nack strategy).
func kafkaKinds(s string) ([]kindReport, error) {
	cfg, err := kafka.ParseDSN(s)
	if err != nil {
		return nil, err
	}
	return []kindReport{
		kindRow("A14", "offset", cfg.Tracker.Resolve().String(), cfg.Tracker != offset.Auto),
		kindRow("A15", "nack", cfg.Nack.Resolve().String(), cfg.Nack != nack.Auto),
	}, nil
}

// kafkagoKinds is kafkaKinds for the second Kafka client library, whose Config
// carries the same two families.
func kafkagoKinds(s string) ([]kindReport, error) {
	cfg, err := kafkago.ParseDSN(s)
	if err != nil {
		return nil, err
	}
	return []kindReport{
		kindRow("A14", "offset", cfg.Tracker.Resolve().String(), cfg.Tracker != offset.Auto),
		kindRow("A15", "nack", cfg.Nack.Resolve().String(), cfg.Nack != nack.Auto),
	}, nil
}

// sqsKinds reports (the settlement flush kind).
func sqsKinds(s string) ([]kindReport, error) {
	cfg, err := sqs.ParseDSN(s)
	if err != nil {
		return nil, err
	}
	return []kindReport{
		kindRow("A32", "flush", cfg.Settle.Resolve().String(), cfg.Settle != flush.Auto),
	}, nil
}

// snssqsKinds is sqsKinds for the SNS-fan-out profile.
func snssqsKinds(s string) ([]kindReport, error) {
	cfg, err := snssqs.ParseDSN(s)
	if err != nil {
		return nil, err
	}
	return []kindReport{
		kindRow("A32", "flush", cfg.Settle.Resolve().String(), cfg.Settle != flush.Auto),
	}, nil
}

// redisKinds reports (the Redis Streams nack strategy). Its DSN
// parameter is spelled `nack=` like Kafka's, and it is a DIFFERENT family with
// different kinds: a pending-entry-list strategy is not a partition-blocking
// one. Reporting the family id beside the kind is what keeps the two apart.
func redisKinds(s string) ([]kindReport, error) {
	cfg, err := redis.ParseDSN(s)
	if err != nil {
		return nil, err
	}
	return []kindReport{
		kindRow("A16", "streamsnack", cfg.Nack.Resolve().String(), cfg.Nack != streams.Auto),
	}, nil
}

// rabbitKinds reports families (publisher confirms), (delayed
// delivery) and (the reconnect backoff).
func rabbitKinds(s string) ([]kindReport, error) {
	cfg, err := rabbitmq.ParseDSN(s)
	if err != nil {
		return nil, err
	}
	return []kindReport{
		kindRow("A33", "confirm", cfg.ConfirmKind.Resolve().String(), cfg.ConfirmKind != confirm.Auto),
		kindRow("A17", "rabbitdelay", cfg.DelayKind.Resolve().String(), cfg.DelayKind != delay.Auto),
		kindRow("A21", "backoff", cfg.BackoffPolicy.Resolve().String(), cfg.BackoffPolicy != backoff.Auto),
	}, nil
}
