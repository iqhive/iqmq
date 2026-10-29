// Package idempotency is the message contract's answer to the sentence "the
// portable API does not provide exactly-once processing": a claim store, a
// middleware over it, and a written account of what the claim is and is not
// worth.
//
// It is experimental (`x/`), so its surface may change; what it must not do is
// change quietly, which is why every claim it makes is stated here and held by
// a test.
//
// # What the store does
//
// A [Store] holds one CLAIM per key for a bounded WINDOW. A claim has two
// phases and both are necessary:
//
//	c, err := store.Claim(ctx, key, window)   // phase 1: reserve
//	switch c.Status {
//	case StatusFirst:      // nobody has done this work; it is yours
//	case StatusDuplicate:  // it was done inside the window; skip it
//	case StatusInFlight:   // somebody else has it RIGHT NOW; come back later
//	case StatusUnknown:    // the store could not answer
//	}
//	store.Commit(ctx, c)                      // phase 2: it is done
//	store.Abort(ctx, c)                        // ...or it is not, release it
//
// One phase would be a defect rather than a simplification. A single
// test-and-set marks the key done BEFORE the work happens, so a worker that
// dies mid-handler leaves a key that says "done" over work that never
// happened, and the redelivery — the one mechanism that would have fixed
// it — is the thing the store then suppresses. That failure is silent, it is
// permanent, and it is indistinguishable from success. So a claim is opened,
// the work runs, and the claim is committed or aborted.
//
// # StatusInFlight is not a duplicate, and that is the sharpest rule here
//
// A key another worker holds an OPEN claim on is not a key whose work is done.
// Treating it as a duplicate loses the work outright: the handler is skipped,
// the delivery is acked, and if the holder then dies the message nobody
// processed is gone. So [StatusInFlight] is "retry this delivery later" and
// never "skip it": [Middleware] returns [mq.Postpone], and the runner offers
// the delivery again after [Config.InFlightRetryAfter], or its own backoff,
// without spending an attempt.
//
// # A duplicate is acked here
//
// [Middleware] acks a [StatusDuplicate] delivery itself rather than leaving
// it to the runner: the handler never sees it, so under mq.SettlementManual
// nothing else would settle it, and a duplicate left in flight is redelivered
// each time its lease runs out.
//
// # A handler that settles its own delivery
//
// [Middleware] commits a claim when the handler returns nil and either left
// the settlement to the runner or acked the delivery itself
// ([mq.Delivery.Acked]). A handler that nacks or rejects its own delivery and
// returns nil has asked for the message again, so its claim is aborted, as
// for a failure, and the redelivery is processed rather than skipped as a
// duplicate.
//
// # What the window is worth
//
// A verdict of [StatusFirst] is not a proof that the key is new. It is the
// store saying "I have no record of this key", and a store forgets for four
// reasons, only the first of which is a decision:
//
//  1. the window lapsed. That is the point of the window: the store is not a
//     ledger and never grows without bound.
//  2. the entry was EVICTED under memory pressure before its window lapsed.
//     [MemoryStore] counts these ([Stats.Evicted]); a Redis configured with
//     `maxmemory-policy allkeys-lru` does the same thing and counts nothing —
//     see the redisstore package's own documentation, because there the
//     effective window is not the TTL and the operator sets it by accident.
//  3. the store lost the write. Asynchronous replication plus a failover, a
//     restart without persistence, a `FLUSHDB`.
//  4. the key is not the one the caller meant. Key derivation is the
//     APPLICATION's ([Config.Key] has no default for exactly this reason): two
//     distinct messages that derive one key are a false duplicate, and one
//     message that derives two keys is a false first.
//
// Numbers 1 to 3 produce a FALSE FIRST — a duplicate the store lets through —
// which degrades to the at-least-once delivery the substrate already promised,
// so the handler's own effect must still be safe to repeat. Number 4 can
// produce a FALSE DUPLICATE, which DISCARDS work, and it is the only one of
// the four that can lose a message. That asymmetry is why [Config.Key] is
// required rather than guessed.
//
// # What happens when the store is unreachable
//
// [OnUnavailableProcess] (the default) runs the handler. [OnUnavailableRetry]
// nacks the delivery. Both costs are real and neither is hidden; the trade is
// [Config.OnUnavailable]'s documentation, and the specification records why the
// default is the one it is.
package idempotency
