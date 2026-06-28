package main

import (
	"bitbucket.org/iqhive/iqmq/codec"
	"bitbucket.org/iqhive/iqmq/driverkit/flush"
	"bitbucket.org/iqhive/iqmq/driverkit/nack"
	"bitbucket.org/iqhive/iqmq/driverkit/offset"
	activemqproto "bitbucket.org/iqhive/iqmq/internal/activemq/protocol"
	"bitbucket.org/iqhive/iqmq/internal/backoff"
	"bitbucket.org/iqhive/iqmq/internal/batch"
	"bitbucket.org/iqhive/iqmq/internal/completion"
	"bitbucket.org/iqhive/iqmq/internal/corr"
	dispatchfam "bitbucket.org/iqhive/iqmq/internal/dispatch"
	"bitbucket.org/iqhive/iqmq/internal/engine"
	"bitbucket.org/iqhive/iqmq/internal/hash"
	"bitbucket.org/iqhive/iqmq/internal/id"
	"bitbucket.org/iqhive/iqmq/internal/leasectx"
	"bitbucket.org/iqhive/iqmq/internal/mwchain"
	"bitbucket.org/iqhive/iqmq/internal/pool"
	"bitbucket.org/iqhive/iqmq/internal/queue"
	"bitbucket.org/iqhive/iqmq/internal/rabbit/confirm"
	"bitbucket.org/iqhive/iqmq/internal/rabbit/delay"
	rabbitproto "bitbucket.org/iqhive/iqmq/internal/rabbit/protocol"
	"bitbucket.org/iqhive/iqmq/internal/registry"
	"bitbucket.org/iqhive/iqmq/internal/stats"
	"bitbucket.org/iqhive/iqmq/internal/streams"
	"bitbucket.org/iqhive/iqmq/internal/timer"
	"bitbucket.org/iqhive/iqmq/internal/wait"
	"bitbucket.org/iqhive/iqmq/internal/wire"
)

// variantFamily is one of the runtime-selectable variant families, as this
// binary was built with it.
//
// The two things `doctor` reports about a family are different facts. The kind
// bound to Auto is a property of THE BUILD -- a generated constant in the family's own auto.go, chosen by a bake-off campaign -- and the provisional
// flag says whether that campaign was authoritative. What a particular target
// resolved to is a property of THE DSN, and lives in the driver's own typed
// configuration; [driverEntry.resolved] reports that half.
//
// Nothing generates this table, so nothing stops it going stale except
// TestVariantTableCoversEveryAutoBinding, which walks the repository's own
// auto.go files and fails on a binding with no row here.
type variantFamily struct {
	// id is the variant id, "A1".
	id string
	// key is the variant's AUTO_KEY, which is the
	// name the bake-off and the ADR front matter call it.
	key string
	// pkg is the repository-relative package that holds the binding.
	pkg string
	// auto is the slug Auto resolves to in this build.
	auto string
	// provisional reports that the binding was chosen on a
	// non-authoritative host and must be re-ratified before the v1 defaults
	// freeze. Every family's is true today.
	provisional bool
	// kinds are the concrete kinds' slugs, which is what `bench --variants`
	// sweeps.
	kinds []string
	// param is the DSN parameter that selects a kind for this family, empty
	// for a family no DSN reaches.
	param string
	// schemes are the schemes whose DSN carries that parameter.
	schemes []string
	// dsnExcluded are the kinds a DSN cannot select, with the reason. A kind
	// of a runtime-selectable family is not automatically reachable from a
	// connection string: the specification's `custom` needs Config.Decide, a Go
	// callback, so a DSN naming it is refused by the driver. `bench
	// --variants` sweeps the selectable kinds and PRINTS the excluded ones
	// with the reason, rather than silently running fewer arms than the family has kinds.
	dsnExcluded map[string]string
}

// dsnKinds are the kinds a DSN can actually select, Auto excluded.
//
// Auto is excluded because the zero kind is what "the DSN did not say" means:
// every driver refuses the spelling on purpose, so the arm that measures the
// Auto binding is the one with the parameter left out.
func (f variantFamily) dsnKinds() []string {
	out := make([]string, 0, len(f.kinds))
	for _, k := range f.kinds {
		if k == "auto" {
			continue
		}
		if _, no := f.dsnExcluded[k]; no {
			continue
		}
		out = append(out, k)
	}
	return out
}

// slugs renders a kind list.
func slugs[K interface{ String() string }](ks []K) []string {
	out := make([]string, 0, len(ks))
	for _, k := range ks {
		out = append(out, k.String())
	}
	return out
}

// variantFamilies is every runtime-selectable family this binary contains.
//
// the fourteen Class B rows are deliberately absent: they are mutually
// exclusive API SHAPES, and the winner of each is already the shape the
// frozen API has. There is no kind to resolve at run time and nothing a CLI
// could report beyond what godoc says. Their prototypes were removed at the
// M6 disposition review (ADR-025); docs/api-freeze.md is where a reader
// looks for what each row decided.
func variantFamilies() []variantFamily {
	return []variantFamily{
		{
			id: "A1", key: "queue", pkg: "internal/queue",
			auto: queue.AutoKind.String(), provisional: queue.AutoProvisional,
			kinds: slugs(queue.Kinds()),
		},
		{
			id: "A2", key: "arch", pkg: "internal/engine",
			auto: engine.ArchAutoKind.String(), provisional: engine.ArchAutoProvisional,
			kinds: slugs(engine.Arches()),
		},
		{
			id: "A3", key: "wait", pkg: "internal/wait",
			auto: wait.AutoKind.String(), provisional: wait.AutoProvisional,
			kinds: slugs(wait.Kinds()),
		},
		{
			id: "A4", key: "timer", pkg: "internal/timer",
			auto: timer.AutoKind.String(), provisional: timer.AutoProvisional,
			kinds: slugs(timer.Kinds()),
		},
		{
			id: "A5", key: "hash", pkg: "internal/hash",
			auto: hash.AutoKind.String(), provisional: hash.AutoProvisional,
			kinds: slugs(hash.Kinds()),
		},
		{
			id: "A6", key: "pool", pkg: "internal/pool",
			auto: pool.AutoKind.String(), provisional: pool.AutoProvisional,
			kinds: slugs(pool.Kinds()),
		},
		{
			id: "A7", key: "fanout", pkg: "internal/engine",
			auto: engine.FanoutAutoKind.String(), provisional: engine.FanoutAutoProvisional,
			kinds: slugs(engine.FanoutPolicies()),
		},
		{
			id: "A8", key: "dispatch", pkg: "internal/dispatch",
			auto: dispatchfam.AutoKind.String(), provisional: dispatchfam.AutoProvisional,
			kinds: slugs(dispatchfam.Kinds()),
		},
		{
			id: "A9", key: "stats", pkg: "internal/stats",
			auto: stats.AutoKind.String(), provisional: stats.AutoProvisional,
			kinds: slugs(stats.Kinds()),
		},
		{
			id: "A13", key: "activemqprotocol", pkg: "internal/activemq/protocol",
			auto: activemqproto.AutoKind.String(), provisional: activemqproto.AutoProvisional,
			kinds: slugs(activemqproto.Kinds()),
		},
		{
			id: "A14", key: "offset", pkg: "driverkit/offset",
			auto: offset.AutoKind.String(), provisional: offset.AutoProvisional,
			kinds: slugs(offset.Kinds()),
			param: "tracker", schemes: []string{"kafka", "kafkago"},
		},
		{
			id: "A15", key: "nack", pkg: "driverkit/nack",
			auto: nack.AutoKind.String(), provisional: nack.AutoProvisional,
			kinds: slugs(nack.Kinds()),
			param: "nack", schemes: []string{"kafka", "kafkago"},
			dsnExcluded: map[string]string{
				"custom": "needs Config.Decide, a Go callback a connection string cannot carry",
			},
		},
		{
			id: "A16", key: "streamsnack", pkg: "internal/streams",
			auto: streams.AutoKind.String(), provisional: streams.AutoProvisional,
			kinds: slugs(streams.Kinds()),
			param: "nack", schemes: []string{"redis"},
		},
		{
			id: "A17", key: "rabbitdelay", pkg: "internal/rabbit/delay",
			auto: delay.AutoKind.String(), provisional: delay.AutoProvisional,
			kinds: slugs(delay.Kinds()),
			param: "delay", schemes: []string{"rabbitmq"},
		},
		{
			id: "A18", key: "wire", pkg: "internal/wire",
			auto: wire.AutoKind.String(), provisional: wire.AutoProvisional,
			kinds: slugs(wire.Kinds()),
		},
		{
			id: "A19", key: "id", pkg: "internal/id",
			auto: id.AutoKind.String(), provisional: id.AutoProvisional,
			kinds: slugs(id.Kinds()),
		},
		{
			id: "A20", key: "codec", pkg: "codec",
			auto: codec.AutoKind.String(), provisional: codec.AutoProvisional,
			kinds: slugs(codec.Kinds()),
		},
		{
			id: "A21", key: "backoff", pkg: "internal/backoff",
			auto: backoff.AutoKind.String(), provisional: backoff.AutoProvisional,
			kinds: slugs(backoff.Kinds()),
			param: "backoff", schemes: []string{"rabbitmq"},
		},
		{
			id: "A22", key: "completion", pkg: "internal/completion",
			auto: completion.AutoKind.String(), provisional: completion.AutoProvisional,
			kinds: slugs(completion.Kinds()),
		},
		{
			id: "A23", key: "batch", pkg: "internal/batch",
			auto: batch.AutoKind.String(), provisional: batch.AutoProvisional,
			kinds: slugs(batch.Kinds()),
		},
		{
			id: "A25", key: "leasectx", pkg: "internal/leasectx",
			auto: leasectx.AutoKind.String(), provisional: leasectx.AutoProvisional,
			kinds: slugs(leasectx.Kinds()),
		},
		{
			id: "A27", key: "registry", pkg: "internal/registry",
			auto: registry.AutoKind.String(), provisional: registry.AutoProvisional,
			kinds: slugs(registry.Kinds()),
		},
		{
			id: "A28", key: "retry", pkg: "internal/engine",
			auto: engine.RetryLaneAutoKind.String(), provisional: engine.RetryLaneAutoProvisional,
			kinds: slugs(engine.RetryLanes()),
		},
		{
			id: "A30", key: "mwchain", pkg: "internal/mwchain",
			auto: mwchain.AutoKind.String(), provisional: mwchain.AutoProvisional,
			kinds: slugs(mwchain.Kinds()),
		},
		{
			id: "A31", key: "corr", pkg: "internal/corr",
			auto: corr.AutoKind.String(), provisional: corr.AutoProvisional,
			kinds: slugs(corr.Kinds()),
		},
		{
			id: "A32", key: "flush", pkg: "driverkit/flush",
			auto: flush.AutoKind.String(), provisional: flush.AutoProvisional,
			kinds: slugs(flush.Kinds()),
			param: "settle", schemes: []string{"sqs", "snssqs"},
		},
		{
			id: "A33", key: "confirm", pkg: "internal/rabbit/confirm",
			auto: confirm.AutoKind.String(), provisional: confirm.AutoProvisional,
			kinds: slugs(confirm.Kinds()),
			param: "confirm", schemes: []string{"rabbitmq"},
		},
		{
			id: "A35", key: "rabbitprotocol", pkg: "internal/rabbit/protocol",
			auto: rabbitproto.AutoKind.String(), provisional: rabbitproto.AutoProvisional,
			kinds: slugs(rabbitproto.Kinds()),
		},
	}
}

// provisionalCount is how many of the families are still provisionally bound.
// `doctor` reports it as one number as well as per family, because "every
// default in this binary is provisional" is the fact a reader needs first and
// it is invisible in a table of twenty-eight rows that all say the same thing.
func provisionalCount() (provisional, total int) {
	fams := variantFamilies()
	for i := range fams {
		total++
		if fams[i].provisional {
			provisional++
		}
	}
	return provisional, total
}

// sweepsFor returns the families a scheme can sweep through its DSN, which is
// what `bench --variants` runs one arm per kind of.
func sweepsFor(scheme string) []variantFamily {
	fams := variantFamilies()
	out := make([]variantFamily, 0, len(fams))
	for i := range fams {
		if fams[i].param == "" {
			continue
		}
		for _, s := range fams[i].schemes {
			if s == scheme {
				out = append(out, fams[i])
				break
			}
		}
	}
	return out
}

// familyByID finds a family by its id.
func familyByID(idOrKey string) (variantFamily, bool) {
	fams := variantFamilies()
	for i := range fams {
		if fams[i].id == idOrKey || fams[i].key == idOrKey {
			return fams[i], true
		}
	}
	return variantFamily{}, false
}
