// Package settle is the executable reference model of delivery settlement
// (the message contract, the specification D). It mirrors models/tla/Settlement.tla
// in Go: a pure, single-goroutine state machine over a set of deliveries,
// driven by Step, with Invariants checked after every step and a Snapshot for
// traces. internal/settlement is the production implementation and is fuzzed
// differentially against this model; the model imports nothing from it.
//
// Per delivery the model keeps the state (idle, active, settling, settled,
// unknown, closed), the current generation, the state a definite rejection
// returns to (the pending revocation), the racer owning the settlement in
// flight, and the history needed by the invariants: the generations settled
// so far, the generations whose lease was lost while active, and the winners.
package settle

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
)

// State is a settlement state. The enumeration matches internal/settlement.
type State uint8

// The settlement states.
const (
	// Idle holds no lease (zero value, or the lease was revoked).
	Idle State = iota
	// Active is leased; a settlement may begin.
	Active
	// Settling has one terminal attempt in flight.
	Settling
	// Settled is the successful terminal state.
	Settled
	// Unknown is the ambiguous terminal state.
	Unknown
	// Closed records a consumer close while leased.
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

// Kind identifies an operation.
type Kind uint8

// The operations, mirroring the TLA+ actions and the Cell methods.
const (
	// Lease delivers the record under a fresh generation (TLA Lease).
	Lease Kind = iota
	// Begin claims the terminal settlement (TLA Begin).
	Begin
	// Retry is Begin that may also leave Unknown (TLA Retry).
	Retry
	// Commit records a successful native settlement (TLA Commit).
	Commit
	// Reject records a definite native rejection (TLA Reject).
	Reject
	// Ambiguous records an ambiguous native result (TLA Ambiguous).
	Ambiguous
	// Revoke withdraws the lease: expiry or rebalance (TLA Revoke).
	Revoke
	// Close records the owning consumer's close (TLA Close).
	Close

	kindCount
)

var kindNames = [kindCount]string{"lease", "begin", "retry", "commit", "reject", "ambiguous", "revoke", "close"}

// String returns the lower-case operation name.
func (k Kind) String() string {
	if k < kindCount {
		return kindNames[k]
	}
	return "kind(?)"
}

// Op is one operation applied by Step.
type Op struct {
	// Kind selects the operation.
	Kind Kind
	// Cell indexes the delivery.
	Cell int
	// Racer identifies the handle presenting Gen for Begin and Retry; it is
	// recorded as the owner of the attempt and, on Commit, as the winner.
	Racer int
	// Gen is the generation presented. Lease ignores it.
	Gen uint32
}

// Result is what Step returns; it is what the implementation must return.
type Result struct {
	// Err is nil when Begin or Retry won, otherwise the errcode sentinel.
	// It is nil for every other kind.
	Err error
	// OK is the boolean result of Lease, Commit, Reject, Ambiguous, Revoke
	// and Close, and true when Begin or Retry won.
	OK bool
	// Gen is the generation handed out by a successful Lease.
	Gen uint32
	// State is the delivery's state after the operation.
	State State
}

// Winner records a successful terminal settlement.
type Winner struct {
	Gen   uint32
	Racer int
}

// Delivery is the model of one settlement cell.
type Delivery struct {
	State State
	// Pending is the state a definite rejection returns to: Active when no
	// revocation is pending, Idle when the lease was revoked while settling,
	// Closed when the consumer closed while settling.
	Pending State
	Gen     uint32
	// Owner is the racer whose attempt is in flight; -1 when none.
	Owner int
	// OwnerGen is the generation Owner presented.
	OwnerGen uint32
	// Settled lists the generations terminally settled, in order.
	Settled []uint32
	// Lost lists the generations whose lease was revoked or closed while
	// Active, in order.
	Lost []uint32
	// Winners lists every successful terminal settlement.
	Winners []Winner
}

func (d *Delivery) clone() Delivery {
	c := *d
	c.Settled = append([]uint32(nil), d.Settled...)
	c.Lost = append([]uint32(nil), d.Lost...)
	c.Winners = append([]Winner(nil), d.Winners...)
	return c
}

// Model is the settlement reference model over a fixed set of deliveries.
type Model struct {
	cells []Delivery
	prev  []Delivery
	open  int
	now   time.Duration
	steps int
}

// New returns a model of n deliveries, all Idle under generation 0.
func New(n int) *Model {
	m := &Model{cells: make([]Delivery, n), prev: make([]Delivery, n)}
	for i := range m.cells {
		m.cells[i].Pending = Active
		m.cells[i].Owner = -1
	}
	copy(m.prev, m.cells)
	return m
}

// Cells returns the number of deliveries.
func (m *Model) Cells() int { return len(m.cells) }

// Advance moves the model's logical clock. Settlement is time-free (leases
// expire through Revoke, driven by the caller), so Advance changes no
// delivery; it exists so every reference model shares one surface.
func (m *Model) Advance(dt time.Duration) {
	if dt > 0 {
		m.now += dt
	}
}

// Now returns the logical clock.
func (m *Model) Now() time.Duration { return m.now }

// Steps returns the number of operations applied.
func (m *Model) Steps() int { return m.steps }

// nextGen mirrors settlement.Cell.Lease: increment, never 0.
func nextGen(g uint32) uint32 {
	if g == math.MaxUint32 {
		return 1
	}
	return g + 1
}

// Step applies op and returns the result the implementation must produce.
// An op.Cell out of range or an undefined op.Kind is a harness error, not a
// behaviour of the system, and is reported in Result.Err.
func (m *Model) Step(op Op) Result {
	if op.Cell < 0 || op.Cell >= len(m.cells) {
		return Result{Err: fmt.Errorf("settle: cell index %d out of range", op.Cell)}
	}
	for i := range m.cells {
		m.prev[i] = m.cells[i].clone()
	}
	m.steps++
	d := &m.cells[op.Cell]
	var res Result
	switch op.Kind {
	case Lease:
		if d.State != Active && d.State != Settling {
			d.Gen = nextGen(d.Gen)
			d.State = Active
			d.Pending = Active
			res.OK, res.Gen = true, d.Gen
		}
	case Begin, Retry:
		from := d.State == Active || (op.Kind == Retry && d.State == Unknown)
		if from && op.Gen == d.Gen {
			d.State = Settling
			d.Owner, d.OwnerGen = op.Racer, op.Gen
			m.open++
			res.OK = true
		} else {
			res.Err = failure(d, op.Gen)
		}
	case Commit:
		if d.State == Settling && op.Gen == d.Gen {
			d.State, d.Pending = Settled, Active
			d.Settled = append(d.Settled, d.Gen)
			d.Winners = append(d.Winners, Winner{Gen: d.Gen, Racer: d.Owner})
			d.Owner = -1
			m.open--
			res.OK = true
		}
	case Reject:
		if d.State == Settling && op.Gen == d.Gen {
			d.State, d.Pending = d.Pending, Active
			d.Owner = -1
			m.open--
			res.OK = true
		}
	case Ambiguous:
		if d.State == Settling && op.Gen == d.Gen {
			d.State, d.Pending = Unknown, Active
			d.Owner = -1
			m.open--
			res.OK = true
		}
	case Revoke:
		if op.Gen == d.Gen {
			switch d.State {
			case Active:
				d.State = Idle
				d.Lost = append(d.Lost, d.Gen)
				res.OK = true
			case Settling:
				if d.Pending == Active {
					d.Pending = Idle
				}
			default:
			}
		}
	case Close:
		if op.Gen == d.Gen {
			switch d.State {
			case Active:
				d.State = Closed
				d.Lost = append(d.Lost, d.Gen)
				res.OK = true
			case Settling:
				d.Pending = Closed
			default:
			}
		}
	default:
		return Result{Err: fmt.Errorf("settle: undefined operation kind %d", op.Kind)}
	}
	res.State = d.State
	return res
}

// failure is the errcode result of a Begin or Retry that did not win.
func failure(d *Delivery, gen uint32) error {
	if gen != d.Gen {
		return errcode.ErrLeaseLost
	}
	switch d.State {
	case Settling:
		return errcode.ErrSettlementInProgress
	case Settled:
		return errcode.ErrAlreadySettled
	case Unknown:
		return errcode.ErrSettlementUnknown
	case Closed:
		return errcode.ErrClosed
	default: // Idle
		return errcode.ErrLeaseLost
	}
}

// Invariants checks the model after a step and returns the first violation.
// Each check carries the name of the TLA+ invariant or property it mirrors
// (models/tla/Settlement.cfg; tools/modelcheck cross-references the names):
//
//   - TypeOK: every state, pending target and owner is well formed; a
//     pending revocation and an owner exist only while Settling.
//   - OneWinner: at most one attempt is in flight per delivery, and the
//     number of attempts in flight equals the number of Settling deliveries.
//   - NoDoubleTerminal: no generation is terminally settled twice, and a
//     Settled delivery's current generation is the one last settled.
//   - StaleRejected: the attempt in flight presents the current generation,
//     and every winner settled the generation current at the time.
//   - LeaseLostNeverSettles: a generation whose lease was revoked or closed
//     while Active is never settled.
//   - UnknownSticky: Unknown is left only through Retry (to Settling) or
//     Lease (to Active).
//   - SettledSticky: Settled is left only through Lease.
//   - ClosedSticky: Closed is left only through Lease.
//   - GenMonotone: the generation is unchanged or advanced by one lease.
//
// The errcode results themselves (which sentinel a losing Begin returns) are
// defined by Step and checked against the implementation by the differential
// fuzz target rather than by an invariant.
func (m *Model) Invariants() error {
	settling := 0
	for i := range m.cells {
		d := &m.cells[i]
		p := &m.prev[i]
		if d.State >= stateCount || d.Pending >= stateCount {
			return fmt.Errorf("TypeOK: cell %d has undefined state %d/%d", i, d.State, d.Pending)
		}
		if d.Pending != Active && d.Pending != Idle && d.Pending != Closed {
			return fmt.Errorf("TypeOK: cell %d has pending target %v", i, d.Pending)
		}
		if d.State != Settling && (d.Pending != Active || d.Owner != -1) {
			return fmt.Errorf("TypeOK: cell %d is %v with pending %v owner %d", i, d.State, d.Pending, d.Owner)
		}
		if d.State == Settling {
			settling++
			if d.Owner < 0 {
				return fmt.Errorf("TypeOK: cell %d is settling without an owner", i)
			}
			if d.OwnerGen != d.Gen {
				return fmt.Errorf("StaleRejected: cell %d is settling gen %d but the owner presented %d", i, d.Gen, d.OwnerGen)
			}
		}
		seen := make(map[uint32]bool, len(d.Settled))
		for _, g := range d.Settled {
			if seen[g] {
				return fmt.Errorf("NoDoubleTerminal: cell %d settled generation %d twice", i, g)
			}
			seen[g] = true
		}
		if d.State == Settled && (len(d.Settled) == 0 || d.Settled[len(d.Settled)-1] != d.Gen) {
			return fmt.Errorf("NoDoubleTerminal: cell %d is settled but generation %d is not the last settled", i, d.Gen)
		}
		if len(d.Winners) != len(d.Settled) {
			return fmt.Errorf("StaleRejected: cell %d has %d winners for %d settlements", i, len(d.Winners), len(d.Settled))
		}
		for j, w := range d.Winners {
			if w.Gen != d.Settled[j] {
				return fmt.Errorf("StaleRejected: cell %d winner %d settled generation %d, recorded %d", i, j, w.Gen, d.Settled[j])
			}
		}
		for _, g := range d.Lost {
			if seen[g] {
				return fmt.Errorf("LeaseLostNeverSettles: cell %d settled generation %d after its lease was lost", i, g)
			}
		}
		switch p.State {
		case Unknown:
			if d.State != Unknown && d.State != Settling && d.State != Active {
				return fmt.Errorf("UnknownSticky: cell %d left unknown for %v", i, d.State)
			}
		case Settled:
			if d.State != Settled && d.State != Active {
				return fmt.Errorf("SettledSticky: cell %d left settled for %v", i, d.State)
			}
		case Closed:
			if d.State != Closed && d.State != Active {
				return fmt.Errorf("ClosedSticky: cell %d left closed for %v", i, d.State)
			}
		default:
		}
		if d.Gen != p.Gen && d.Gen != nextGen(p.Gen) {
			return fmt.Errorf("GenMonotone: cell %d generation went %d -> %d", i, p.Gen, d.Gen)
		}
		if d.Gen != p.Gen && d.State != Active {
			return fmt.Errorf("GenMonotone: cell %d changed generation without a lease", i)
		}
	}
	if settling != m.open {
		return fmt.Errorf("OneWinner: %d attempts in flight but %d deliveries settling", m.open, settling)
	}
	return nil
}

// ErrNoSuchCell is returned by Delivery for an index out of range.
var ErrNoSuchCell = errors.New("settle: no such cell")

// Delivery returns a copy of cell i.
func (m *Model) Delivery(i int) (Delivery, error) {
	if i < 0 || i >= len(m.cells) {
		return Delivery{}, ErrNoSuchCell
	}
	return m.cells[i].clone(), nil
}

// Snapshot is a copy of the whole model.
type Snapshot struct {
	Cells []Delivery
	Now   time.Duration
	Steps int
}

// Snapshot returns a deep copy of the model state.
func (m *Model) Snapshot() Snapshot {
	s := Snapshot{Cells: make([]Delivery, len(m.cells)), Now: m.now, Steps: m.steps}
	for i := range m.cells {
		s.Cells[i] = m.cells[i].clone()
	}
	return s
}
