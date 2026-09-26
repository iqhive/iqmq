// Package settlement implements the per-delivery settlement state machine of
// the message contract as one 64-bit compare-and-swap word.
//
//	active ──► settling ──► settled
//	              ├─► active              (definite rejection; retry allowed)
//	              └─► settlement-unknown  (ambiguous native result)
//
// A Cell packs the state, the delivery generation and the pending-revocation
// flags into one word, so every rule of the specification is a single CAS:
// exactly one concurrent terminal attempt wins and the loser sees
// ErrSettlementInProgress; a second terminal action after success sees
// ErrAlreadySettled; a stale generation sees ErrLeaseLost; a settlement after
// the owning consumer closed sees ErrClosed; an ambiguous native outcome
// stays Unknown (ErrSettlementUnknown) until a protocol-safe Retry. Every
// result is an internal/errcode sentinel: the package never allocates.
//
// # Generations
//
// Lease hands out generation g+1 for every delivery or redelivery of the
// record the cell guards and never hands out 0, so a zero Gen or a zero Token
// can never match a live lease, and a handle from an earlier delivery cannot
// settle a later one, including after the owning envelope is recycled through
// a pool: a cell is never reset, only re-leased.
//
// # Pending revocation
//
// Revoke (lease expiry, rebalance) and Close (consumer close) claim an Active
// lease immediately. When they meet a settlement in flight they record their
// intent instead: the winner's Commit or Ambiguous stands, and a definite
// Reject lands in Idle or Closed rather than back in Active, so a lease that
// was lost while a native round trip was in flight can never be retried
// against. Close takes precedence over Revoke.
//
// # Models
//
// The executable reference model is internal/model/settle and the TLA+
// specification is models/tla/Settlement.tla; models/tla/TRACE.md maps both
// onto this package.
package settlement

import (
	"sync/atomic"

	"github.com/iqhive/iqmq/internal/errcode"
)

// State is the settlement state of one delivery. The zero value is Idle.
type State uint8

// The settlement states. Their numeric values are part of the model contract
// (internal/model/settle uses the same enumeration).
const (
	// Idle holds no lease: the zero value, or the lease was revoked before a
	// settlement began. Begin and Retry return ErrLeaseLost.
	Idle State = iota
	// Active is leased to a consumer; a terminal settlement may begin.
	Active
	// Settling has one terminal attempt in flight; other attempts return
	// ErrSettlementInProgress.
	Settling
	// Settled is the successful terminal state; further attempts return
	// ErrAlreadySettled. Only Lease leaves it.
	Settled
	// Unknown records an ambiguous native result; attempts return
	// ErrSettlementUnknown. Only Retry (same generation) or Lease leave it.
	Unknown
	// Closed records that the owning consumer closed while the delivery was
	// leased; attempts return ErrClosed. Only Lease leaves it.
	Closed

	stateCount
)

var stateNames = [stateCount]string{"idle", "active", "settling", "settled", "unknown", "closed"}

// String returns the lower-case state name.
func (s State) String() string {
	if s < stateCount {
		return stateNames[s]
	}
	return "state(?)"
}

// Valid reports whether s is one of the defined states.
func (s State) Valid() bool { return s < stateCount }

// Gen is a delivery generation. Every Lease of a cell yields a new
// generation; a settlement presenting any other generation is stale and
// fails with ErrLeaseLost. Generation 0 is never leased.
type Gen uint32

// Word layout of a Cell: bits 0–2 state, bit 8 revoke pending, bit 9 close
// pending, bits 32–63 generation. The pending bits are set only while the
// state is Settling.
const (
	stateMask  = 0x7
	pendRevoke = 1 << 8
	pendClose  = 1 << 9
	pendMask   = pendRevoke | pendClose
	genShift   = 32
)

// pack builds a word. pend is the state a definite rejection returns to:
// Active (nothing pending), Idle (revoke pending) or Closed (close pending).
func pack(st, pend State, gen Gen) uint64 {
	w := uint64(gen)<<genShift | uint64(st)
	switch pend {
	case Idle:
		w |= pendRevoke
	case Closed:
		w |= pendClose
	default:
	}
	return w
}

// unpack decodes a word into state, pending target and generation.
func unpack(w uint64) (st, pend State, gen Gen) {
	st = State(w & stateMask)
	pend = Active
	switch {
	case w&pendClose != 0:
		pend = Closed
	case w&pendRevoke != 0:
		pend = Idle
	}
	return st, pend, Gen(w >> genShift)
}

// Cell is the settlement word of one delivery record. The zero value is Idle
// with generation 0 and is ready to use. A Cell must not be copied after
// first use.
//
// All methods are safe for concurrent use from any goroutine and are
// wait-free apart from bounded CAS retries against concurrent writers of the
// same cell. None of them allocates.
type Cell struct {
	w atomic.Uint64
}

// Load returns the current state and generation.
func (c *Cell) Load() (State, Gen) {
	st, _, gen := unpack(c.w.Load())
	return st, gen
}

// Lease starts a new delivery of the record: the cell becomes Active under a
// fresh generation, which is returned. It fails, leaving the cell unchanged,
// when a lease is already held (Active or Settling); the engine must Revoke
// or Close the previous lease first. Pending flags are cleared.
//
// Hot path: one load and one CAS.
func (c *Cell) Lease() (Gen, bool) {
	for {
		old := c.w.Load()
		st, _, gen := unpack(old)
		if st == Active || st == Settling {
			return 0, false
		}
		gen++
		if gen == 0 {
			gen = 1
		}
		if c.w.CompareAndSwap(old, pack(Active, Active, gen)) {
			return gen, true
		}
	}
}

// Begin claims the terminal settlement of the delivery identified by gen:
// Active(gen) becomes Settling(gen) and Begin returns nil. The caller then
// performs the native settlement and reports its outcome with exactly one of
// Commit, Reject or Ambiguous.
//
// Otherwise the cell is unchanged and the error names the reason:
// ErrLeaseLost when gen is not the current generation or the lease was
// revoked (Idle); ErrSettlementInProgress when another attempt is in flight;
// ErrAlreadySettled after a successful settlement; ErrSettlementUnknown
// after an ambiguous one; ErrClosed after the consumer closed.
//
// Hot path: one CAS on success; a load on failure. Never allocates.
func (c *Cell) Begin(gen Gen) error {
	want := pack(Active, Active, gen)
	next := pack(Settling, Active, gen)
	if c.w.CompareAndSwap(want, next) {
		return nil
	}
	for {
		old := c.w.Load()
		if old != want {
			return failure(old, gen)
		}
		if c.w.CompareAndSwap(old, next) {
			return nil
		}
	}
}

// Retry is Begin extended with permission to leave Unknown: a driver whose
// protocol makes a repeated settlement safe uses it to resolve an ambiguous
// outcome under the same generation. Its results are those of Begin.
func (c *Cell) Retry(gen Gen) error {
	active := pack(Active, Active, gen)
	unknown := pack(Unknown, Active, gen)
	next := pack(Settling, Active, gen)
	for {
		old := c.w.Load()
		if old != active && old != unknown {
			return failure(old, gen)
		}
		if c.w.CompareAndSwap(old, next) {
			return nil
		}
	}
}

// failure maps the observed word to the sentinel Begin returns for it.
func failure(old uint64, gen Gen) error {
	st, _, g := unpack(old)
	if g != gen {
		return errcode.ErrLeaseLost
	}
	switch st {
	case Settling:
		return errcode.ErrSettlementInProgress
	case Settled:
		return errcode.ErrAlreadySettled
	case Unknown:
		return errcode.ErrSettlementUnknown
	case Closed:
		return errcode.ErrClosed
	default: // Idle; Active never reaches here because the CAS would have matched
		return errcode.ErrLeaseLost
	}
}

// Commit records a successful native settlement: Settling(gen) becomes
// Settled(gen), clearing any pending revocation. It returns false, leaving
// the cell unchanged, when the cell is not Settling under gen, which means
// the caller did not win Begin: a protocol violation.
func (c *Cell) Commit(gen Gen) bool { return c.resolve(gen, Settled) }

// Ambiguous records a native settlement whose outcome is unknown:
// Settling(gen) becomes Unknown(gen), clearing any pending revocation. The
// delivery stays Unknown until Retry or Lease. It returns false, leaving the
// cell unchanged, when the cell is not Settling under gen.
func (c *Cell) Ambiguous(gen Gen) bool { return c.resolve(gen, Unknown) }

func (c *Cell) resolve(gen Gen, to State) bool {
	next := pack(to, Active, gen)
	if c.w.CompareAndSwap(pack(Settling, Active, gen), next) {
		return true
	}
	for {
		old := c.w.Load()
		st, _, g := unpack(old)
		if st != Settling || g != gen {
			return false
		}
		if c.w.CompareAndSwap(old, next) {
			return true
		}
	}
}

// Reject records a definite native rejection of the settlement. The cell
// returns to Active(gen) and the handle may retry, unless the lease was
// revoked or closed while the attempt was in flight, in which case the cell
// becomes Idle or Closed respectively and the handle is stale. The resulting
// state is returned with true. When the cell is not Settling under gen the
// observed state is returned with false and the cell is unchanged.
func (c *Cell) Reject(gen Gen) (State, bool) {
	for {
		old := c.w.Load()
		st, pend, g := unpack(old)
		if st != Settling || g != gen {
			return st, false
		}
		if c.w.CompareAndSwap(old, pack(pend, Active, gen)) {
			return pend, true
		}
	}
}

// Revoke withdraws the lease of generation gen because it expired or was
// rebalanced away. Active(gen) becomes Idle(gen) and Revoke returns true:
// the caller has claimed the record and must requeue it. When a settlement
// under gen is in flight, the revocation is recorded as pending (unless a
// close is already pending) and false is returned: the settler's Reject
// will land in Idle. In every other case the cell is unchanged and false is
// returned.
func (c *Cell) Revoke(gen Gen) bool {
	for {
		old := c.w.Load()
		st, pend, g := unpack(old)
		if g != gen {
			return false
		}
		switch st {
		case Active:
			if c.w.CompareAndSwap(old, pack(Idle, Active, gen)) {
				return true
			}
		case Settling:
			if pend != Active || c.w.CompareAndSwap(old, pack(Settling, Idle, gen)) {
				return false
			}
		default:
			return false
		}
	}
}

// Close records that the consumer owning generation gen closed. Active(gen)
// becomes Closed(gen) and Close returns true: the caller has claimed the
// record and requeues or releases it per policy. When a settlement under gen
// is in flight, the close is recorded as pending (taking precedence over a
// pending revoke) and false is returned: the settler's Reject will land in
// Closed. In every other case the cell is unchanged and false is returned.
func (c *Cell) Close(gen Gen) bool {
	for {
		old := c.w.Load()
		st, pend, g := unpack(old)
		if g != gen {
			return false
		}
		switch st {
		case Active:
			if c.w.CompareAndSwap(old, pack(Closed, Active, gen)) {
				return true
			}
		case Settling:
			if pend == Closed || c.w.CompareAndSwap(old, pack(Settling, Closed, gen)) {
				return false
			}
		default:
			return false
		}
	}
}

// Token is the packed settlement handle of the "Settler + token" shape:
// the low 32 bits index a slot in a table of cells, the high 32 bits carry
// the generation the holder presents to Begin. The zero Token names slot 0
// under generation 0, which is never leased, so it can never settle anything.
type Token uint64

// NewToken packs slot and gen.
func NewToken(slot uint32, gen Gen) Token { return Token(uint64(gen)<<genShift | uint64(slot)) }

// Slot returns the slot index.
func (t Token) Slot() uint32 { return uint32(t) }

// Gen returns the generation.
func (t Token) Gen() Gen { return Gen(t >> genShift) }
