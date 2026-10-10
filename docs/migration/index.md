# Migration

Three guides, one per starting point. Each is written the same way: what maps
directly, what does not map at all, the traps, and **what a reader loses** —
because the honest limits are the reason anything else on the page can be
trusted.

* **[From Watermill](watermill.md)** — the closest neighbour. The port is
  mostly mechanical; the two things that are not are metadata (a
  `map[string]string` against ordered, duplicate-preserving, binary-capable
  headers) and acknowledgement (`bool` against an error with portable codes,
  including `mq.ErrNotSettleable` for a transport that has no ack). What you
  lose: `message.Router`'s process-wide handler graph, a `<-chan *Message`
  subscription in the primary API, and the freedom not to notice that a
  backend cannot do what you asked.
* **[From gocloud.dev/pubsub](gocloud.md)** — the one where a behaviour
  changes silently if you do not read the page. gocloud *transforms* metadata
  a backend cannot carry (`awssnssqs` hex-escapes keys and URL-escapes
  values); iqmq refuses it in the default metadata mode. A mixed fleet of
  gocloud and iqmq readers on one topic needs a decision before the port. What
  you lose: URL-string-first configuration, implicit topology creation, and
  gocloud's other five packages, which iqmq has no counterpart for at all.
* **[From a raw client](raw-clients.md)** — the expensive one, and the page
  that starts with the measurement rather than the mapping. The batched
  overhead budget is **breached on 4 of 12 measured rows today** and undecided
  on 8 (10 of 12 on the point estimates, before), worst
  +46 %, so a high-volume batched publish path gives up throughput; the page
  says so first and lists what you get for it second.

## Two things common to all three

**iqmq makes you name the confirmation boundary.** Every one of these libraries
has a publish call whose boundary is its own default, and every port that
leaves `PublisherConfig.Confirmation` unset gets `mq.ConfirmAccepted` — which
is stronger than what several of them offered and therefore slower. That is not
a regression; it is a question you were not being asked before. The specification has the
modes.

**iqmq refuses instead of emulating, by default.** The capability table
(`client.Capabilities()`) is generated from conformance runs, and the strict
metadata mode fails a publication that would have lost something. The other
three modes — envelope, adaptive, lenient-drop — exist, and each one's cost is
documented where it is defined. Choosing one is part of the migration.

The measured comparison of iqmq, Watermill, gocloud and a raw client at
*equivalent* semantics, with everything that could not be made equivalent
excluded by name, is
the competitor benchmark.
