package idempotency

import (
	"context"
	"errors"
	"time"
)

// Status is what a [Store] says about one key.
type Status int

const (
	// StatusUnknown is the zero value and means the store could not answer:
	// it was unreachable, it timed out, or it replied with something this
	// package does not understand. It is the zero value deliberately — a
	// caller that forgets to look at the status gets the answer that assumes
	// nothing.
	StatusUnknown Status = iota
	// StatusFirst means the store had no record of the key and this caller
	// now holds an open claim on it. The caller must Commit or Abort it.
	//
	// It is not a proof that the key is new: see the package documentation's
	// four reasons a store forgets.
	StatusFirst
	// StatusDuplicate means a claim on this key was COMMITTED inside the
	// window. The work has been done; the caller holds no claim and must
	// neither Commit nor Abort.
	StatusDuplicate
	// StatusInFlight means an open, unexpired claim on this key is held by
	// somebody else. It is NOT a duplicate: the work may never have been
	// done, and skipping it loses the message. The caller holds no claim and
	// should come back later.
	StatusInFlight
)

// String names the status. The strings are part of the observable surface —
// they appear in [Stats] keys, in log lines and in example output — so they
// are spelled out rather than derived.
func (s Status) String() string {
	switch s {
	case StatusFirst:
		return "first"
	case StatusDuplicate:
		return "duplicate"
	case StatusInFlight:
		return "in-flight"
	case StatusUnknown:
		return "unknown"
	}
	return "invalid"
}

// Held reports whether this status means the caller holds an open claim that
// it must Commit or Abort. Exactly one status does.
func (s Status) Held() bool { return s == StatusFirst }

// Claim is one reservation of one key.
//
// It is a value and carries no reference to the store, so it can be logged,
// compared and passed around; [Store.Commit] and [Store.Abort] take it back.
// The zero Claim holds nothing, and handing it to Commit or Abort is
// [ErrNoClaim] rather than a silent no-op: a claim the caller lost track of is
// a bug, and the two places it shows up — a Commit of a duplicate and a Commit
// of a claim somebody else now holds — are the two ways a two-phase claim gets
// used wrongly.
type Claim struct {
	// Key is the claimed key.
	Key string
	// Token identifies THIS claim of the key, so that a Commit or an Abort
	// arriving after the claim lapsed and was re-taken by somebody else can
	// be refused rather than applied to their claim. It is empty for every
	// status but [StatusFirst].
	Token string
	// Status is the store's verdict.
	Status Status
	// Window is the window this claim was taken with, so that [Store.Commit]
	// can start the duplicate-suppression window afresh rather than from
	// whatever is left of the open claim. Without it a handler that took most
	// of its window would commit a claim that suppresses nothing, and the
	// duplicate the caller was promised would arrive a moment later. It is
	// zero for every status but [StatusFirst].
	//
	// Commit reads it from the Claim it is handed, so a caller may raise it
	// first: that is how [Config.InFlightLease] keeps an open claim short
	// while a committed one lasts the whole [Config.Window].
	Window time.Duration
	// Expires is when the open claim lapses, on the store's clock. It is the
	// zero time when the status is not [StatusFirst], and it is the store's
	// view rather than a promise: a store whose clock differs from the
	// caller's, or which evicts early, lapses sooner than this says.
	Expires time.Time
}

// Store holds claims. Implementations must be safe for concurrent use.
//
// Every method takes a context and every method may fail. A failure is not a
// verdict: Claim reports [StatusUnknown] with a non-nil error, and what to do
// about that is the caller's policy ([Config.OnUnavailable]) and never the
// store's.
type Store interface {
	// Claim reserves key for window, and reports what the store knew about
	// it. window is how long an OPEN claim lasts and, unless the caller
	// raises [Claim.Window] before Commit, how long a COMMITTED one is
	// remembered; a non-positive window is [ErrBadWindow].
	//
	// A store may round the window up to its own resolution and says so in
	// its own documentation. It may not round it DOWN silently: a window
	// shorter than the caller asked for is a duplicate the caller was told
	// would be caught.
	Claim(ctx context.Context, key string, window time.Duration) (Claim, error)
	// Commit records that the claimed work is done, and starts the window in
	// which the key reads as [StatusDuplicate]. The window is c.Window, not
	// the one the claim was taken with.
	//
	// A claim whose token no longer matches the store's — it lapsed and
	// somebody else took the key — is [ErrClaimLost] and nothing is written.
	// That is the caller's signal that its own work may now be running twice.
	Commit(ctx context.Context, c Claim) error
	// Abort releases the claim without recording the work as done, so that a
	// redelivery is processed rather than skipped. A claim whose token no
	// longer matches is [ErrClaimLost]; nothing is written, because the key
	// belongs to somebody else and deleting it would release THEIR claim.
	Abort(ctx context.Context, c Claim) error
	// Close releases the store's own resources. It is idempotent, and it does
	// not close a client the caller lent the store (the specification's ownership rule).
	Close() error
}

// The errors this package's stores report. Every one is matched with
// [errors.Is] and none of them is a verdict about a key.
var (
	// ErrBadWindow reports a non-positive window.
	ErrBadWindow = errors.New("idempotency: the claim window must be positive")
	// ErrEmptyKey reports an empty key. An empty key is refused rather than
	// treated as one shared key, which is what a store that accepted it would
	// be: every keyless message a duplicate of the first.
	ErrEmptyKey = errors.New("idempotency: the claim key is empty")
	// ErrNoClaim reports a Commit or an Abort of a Claim that holds nothing —
	// the zero value, or one whose Status is not StatusFirst.
	ErrNoClaim = errors.New("idempotency: this Claim holds no reservation, so there is nothing to commit or abort")
	// ErrClaimLost reports a Commit or an Abort of a claim the store no
	// longer has: it lapsed, it was evicted, or another worker has since
	// claimed the key. The caller's work may be running twice.
	ErrClaimLost = errors.New("idempotency: the claim lapsed or was taken by another worker")
	// ErrClosed reports a call on a closed store.
	ErrClosed = errors.New("idempotency: the store is closed")
)

// validate is the argument check every Store performs, in one place so that
// two implementations cannot disagree about an empty key.
func validate(key string, window time.Duration) error {
	if key == "" {
		return ErrEmptyKey
	}
	if window <= 0 {
		return ErrBadWindow
	}
	return nil
}

// heldClaim checks that c is a claim a caller may commit or abort.
func heldClaim(c Claim) error {
	if !c.Status.Held() || c.Key == "" || c.Token == "" {
		return ErrNoClaim
	}
	return nil
}
