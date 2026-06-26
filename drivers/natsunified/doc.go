// Package natsunified is one entry point to both NATS profiles: NATS Core
// ([bitbucket.org/iqhive/iqmq/drivers/nats]) and JetStream
// ([bitbucket.org/iqhive/iqmq/drivers/jetstream]).
//
// # What it is, and what it deliberately is not
//
// It is a **facade over two drivers, not a third driver**. It dials nothing,
// publishes nothing, settles nothing and maps no subject: every one of those is
// the selected profile's, unchanged. What this package owns is the choice —
// which profile a `nats://` or `natsjs://` DSN selects, which configuration
// that profile is opened with, and what a caller may rely on before the choice
// is made.
//
// That boundary is the whole design and it is deliberate.
// [Open] returns the profile driver's own [bitbucket.org/iqhive/iqmq.Client], and
// [OpenBackend] its own [bitbucket.org/iqhive/iqmq.Backend]: there is no wrapper
// type here, so there is no path on which a facade can lose a capability claim,
// drop an optional SPI surface, or answer [bitbucket.org/iqhive/iqmq.DriverInfo]
// with a name that hides which driver is really serving the caller.
// `Client.Info().Name` therefore says "nats" or "jetstream" and never
// "natsunified", which is what a caller reading a log line needs to know.
//
// # The profile is always stated
//
// There is no default profile. A [Config] with no [Profile]
// is a construction error, and the DSN scheme is the only other way to set one:
//
//	nats://host:4222/prefix         -> ProfileCore      (NATS Core)
//	natsjs://host:4222/prefix?...   -> ProfileJetStream (JetStream)
//
// The two profiles differ on at-most-once versus at-least-once delivery, which
// is the most consequential property a queue has, so a zero value that picked
// one would make that choice by omission. ADR-016 asked what the documented
// default should be; the answer this package gives is that there is none.
//
// # Semantics
//
// Every semantic below is the selected profile's. They are restated here
// because a caller who selects at run time has to be able to read both columns
// at once, and the specification asks a driver page to state them.
//
//	                       ProfileCore                ProfileJetStream
//	delivery               at-most-once               at-least-once
//	settlement none (AckNone; explicit ack/nak/term,
//	                       Settler() is nil and       with a lease and a
//	                       Ack/Nack/Reject answer     redelivery count
//	                       ErrNotSettleable)
//	storage                none                       stream (file or memory)
//	ConfirmAccepted        PING/PONG: the server       the stream's PubAck
//	                       has parsed the publication
//	ConfirmDurable         REFUSED at publisher        the same PubAck as
//	                       construction               ConfirmAccepted
//	at-least-once window   none: a message the        from the PubAck until the
//	                       server could not deliver   delivery is acked; a lease
//	                       is dropped                 that expires redelivers
//	topology               none: a subject exists     a stream, which this
//	                       because somebody           facade never creates
//	                       published to it
//
// # ConfirmDurable
//
// It is a construction error on Core and the normal case on JetStream, and this
// facade does not move it: [Open] resolves the profile, so the sink a publisher
// opens is the profile's own sink and the refusal happens where
// [bitbucket.org/iqhive/iqmq/drivers/nats] puts it — at
// `Client.Publisher`, before a single message exists. A facade that deferred
// the choice to the first publication would have turned the fail-fast into a
// per-message failure; this one cannot, because by the time there is a
// publisher there is no longer a choice to defer.
//
// What the facade adds is one refusal earlier still. [Config.Require] carries
// The specification requirements that are checked against the selected profile's capability
// table inside [Open], so a DSN that selects Core for an application that needs
// a durable boundary fails at the Open rather than at the first publisher:
//
//	cfg, err := natsunified.ParseDSN(os.Getenv("QUEUE_DSN"))
//	if err != nil { return err }
//	cfg.Require = []mq.Requirement{
//		mq.RequireAtLeast(mq.FeatureDurableConfirm, mq.Native),
//		mq.RequireAtLeast(mq.FeatureExplicitAck, mq.Native),
//	}
//	client, err := natsunified.Open(ctx, cfg) // refused on nats://
//
// A DSN cannot carry a requirement, so that is a two-step and not a one-liner.
// It is the recommended shape for every program whose profile comes from
// configuration.
//
// # Capabilities are the profile's, never a union and never an intersection
//
// `Client.Capabilities()` is the selected profile's table, unchanged.
// A union would claim explicit acknowledgement on a broker
// whose `Settler()` is nil; an intersection would withdraw fifteen claims
// JetStream really honours. Neither is a table any deployment has.
//
// What a caller may rely on *before* the profile is known is [Common], which is
// a plain feature list and never a capability table: the features both profiles
// advertise at some level on every deployment. Its levels and its notes differ
// between the profiles even where the feature is shared —
// [bitbucket.org/iqhive/iqmq.FeaturePublisherConfirm] is native on both and means
// "the server parsed it" on one and "the stream committed it" on the other — so
// [Common] answers "may I call this at all", and only `Capabilities()` answers
// "what does it mean here".
//
// # Maturity and tier
//
// Maturity: Experimental. Tier: 1 (both underlying drivers are tier 1). The
// facade adds no I/O of its own, so its tier is the lower of the two it
// delegates to.
package natsunified
