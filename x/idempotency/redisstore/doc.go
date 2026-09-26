// Package redisstore is [idempotency.Store] on Redis: a claim every replica of
// a service can see, and a written account of what Redis makes that claim
// worth.
//
// It runs the same contract as the in-process store —
// [github.com/iqhive/iqmq/x/idempotency/storetest] — so the two answer
// identically or one of them fails a test. What differs is not the semantics
// but the FAILURE MODES, and those are the whole reason to read this page.
//
// # The claim is one key and three Lua scripts
//
// A claim is one string key, `<Prefix><key>`, whose value is `p:<token>` while
// the claim is open and `d:<token>` once it is committed, with a `PX`
// expiry. The three operations are single scripts because each is a
// compare-and-set and Redis has no other way to make one atomic:
//
//	Claim   SET key p:<token> NX PX <window>   -- then GET on the miss, to
//	                                              tell a committed claim from
//	                                              an open one
//	Commit  GET; if the value is this claim's, SET d:<token> PX <window>
//	Abort   GET; if the value is this claim's, DEL
//
// Without the token comparison a Commit arriving after its own claim lapsed
// would mark ANOTHER worker's claim done, and an Abort would delete it. That
// is the Redlock lesson and it is the only reason these are scripts rather
// than three plain commands.
//
// # What Redis gives the window, and what it takes
//
// The window a caller asks for is the key's TTL. Four things make the
// EFFECTIVE window shorter, and only the first is visible to this package:
//
//  1. **Rounding.** `PX` is whole milliseconds, so a window is rounded UP to
//     the next millisecond. Never down: a window shorter than the caller
//     asked for is a duplicate the caller was told would be caught.
//
//  2. **Eviction.** A Redis at `maxmemory` with `maxmemory-policy
//     allkeys-lru` or `allkeys-random` deletes keys that have not expired,
//     and an idempotency claim is exactly the kind of small, cold key an LRU
//     throws away first. Nothing reports it: the claim is simply gone and the
//     key reads as [idempotency.StatusFirst]. This is the single most likely
//     way for this store to be quietly worthless in production, and the
//     operator usually chose the policy for a cache. Use `noeviction` or
//     `volatile-ttl` on the instance — or a separate instance or logical
//     database (see [Options.DB]) — and alert on `evicted_keys`.
//     [Store.EvictionPolicy] reads the running server's policy so a program
//     can refuse to start on a wrong one, and [Options.RequireSafeEviction]
//     makes [Open] do exactly that.
//
//  3. **Failover.** Redis replication is asynchronous. A primary that accepts
//     a Claim and dies before the replica has it promotes a replica that
//     never saw the key, and the work is claimable again. `WAIT` narrows the
//     window and does not close it; Redis Cluster's failover has the same
//     property. A store on a single unreplicated instance loses everything on
//     restart unless AOF is on with `appendfsync everysec` or better — and
//     "everysec" is a second of claims.
//
//  4. **`FLUSHDB`, a restart without persistence, a key-prefix collision with
//     another service.** [Options.Prefix] exists for the third.
//
// Every one of those produces a FALSE FIRST — a duplicate this store lets
// through — which is the direction that degrades to the at-least-once delivery
// the broker already promised. None of them produces a false DUPLICATE, which
// is the direction that would discard work; the only way to get one of those
// is a key-derivation mistake, which is the application's and is why
// [idempotency.Config.Key] has no default.
//
// # Unreachable is not a verdict
//
// Every method reports an error rather than guessing. [idempotency.Claim] with
// a non-nil error carries [idempotency.StatusUnknown], and what to do about
// it is [idempotency.Config.OnUnavailable] — which fails OPEN by default,
// because the alternative turns a cache outage into a consumer that processes
// nothing and, with a max-attempts policy, dead-letters everything it is sent.
//
// # What it costs
//
// One round trip for a key the store has never seen or has already committed
// ([Claim]'s `SET NX` answers on its own in the committed case only when the
// SET succeeds — a miss costs a `GET` inside the same script, so it is still
// one round trip), and one more for the [Commit] at the end of a successful
// handler. Two round trips per first-time message, one per duplicate. No
// timing figure is quoted here: this increment measured none, and the stored
// Redis campaigns under benchmarks/reports/results/ measure the driver rather
// than this store.
package redisstore
