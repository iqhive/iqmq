package differential

import (
	"fmt"
	"strconv"
	"strings"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// Action is what the run does with every delivery of one message.
//
// The settlement is a property of the MESSAGE and not a separate operation,
// and that is the correction of a design defect rather than a preference. With
// ack, nack and drop as ops that settled "the oldest outstanding delivery",
// the message an op reached depended on the order deliveries ARRIVED — so on a
// broker that interleaves two keys differently from the memory engine, `drop`
// discarded a different message on each arm and the relation reported a
// settlement difference that was an artefact of the script. Against RabbitMQ
// the first run said:
//
//	settlement-differs id=m3 key="k0": dropped on memory and acked on rabbitmq
//
// which is not a defect in RabbitMQ; it is a comparison that cannot hold on
// any broker allowed to reorder across keys, which is nearly all of them.
type Action uint8

// Action values.
const (
	// ActionAck acks the first delivery.
	ActionAck Action = iota + 1
	// ActionNackThenAck nacks the first delivery asking for redelivery and
	// acks the next one. It is what exercises the redelivery contract.
	ActionNackThenAck
	// ActionDrop nacks the first delivery with NackDrop: the message is
	// discarded and must not come back.
	ActionDrop
)

// String implements fmt.Stringer.
func (a Action) String() string {
	switch a {
	case ActionAck:
		return "ack"
	case ActionNackThenAck:
		return "nack-then-ack"
	case ActionDrop:
		return "drop"
	}
	return "invalid"
}

// OpKind is one operation of a script.
type OpKind uint8

// OpKind values.
const (
	// OpPublish publishes one message under Op.Key, to be settled with
	// Op.Action.
	OpPublish OpKind = iota + 1
	// OpReceive asks for up to Op.N deliveries and settles each by its own
	// message's action.
	OpReceive
)

// String implements fmt.Stringer.
func (k OpKind) String() string {
	switch k {
	case OpPublish:
		return "publish"
	case OpReceive:
		return "receive"
	}
	return "invalid"
}

// Op is one operation.
type Op struct {
	// Kind selects which of the other fields carry meaning: [OpPublish]
	// reads Key and Action and ignores N, [OpReceive] reads N and ignores
	// the other two. It is what [Op.String] switches on, and every run of
	// both arms replays it unchanged -- an Op whose Kind differed between
	// the arms is the divergence [KindScript] exists to catch.
	//
	// A decoded script never leaves it zero: the decoder always chooses
	// one of the two, and the zero value is the "invalid" [OpKind] rather
	// than a default operation.
	Kind OpKind
	// Key is the key index for OpPublish, in [0, Script.Keys).
	Key int
	// Action is how OpPublish's message is settled.
	Action Action
	// N is how many deliveries OpReceive asks for.
	N int
}

// String implements fmt.Stringer.
func (o Op) String() string {
	switch o.Kind {
	case OpPublish:
		return "publish(k" + strconv.Itoa(o.Key) + "," + o.Action.String() + ")"
	case OpReceive:
		return "receive(" + strconv.Itoa(o.N) + ")"
	}
	return o.Kind.String()
}

// Script is one operation script: the same sequence of operations, replayed on
// every arm.
//
// It is decoded from the fuzzer's bytes ONCE and then run twice, which is the
// property the whole differential rests on. A script decoded per arm — with
// each arm's own reader, or with any decision that reads the arm's state —
// would let the two arms diverge for a reason the relation then reports as a
// broker defect (the KindScript check exists because that is easy to do by
// accident).
type Script struct {
	// Keys is how many distinct keys the script uses.
	Keys int
	// Ops is the operation sequence.
	Ops []Op
}

// MaxOps bounds a decoded script. A differential run costs a broker round trip
// per operation, so the bound is what keeps one fuzz input from becoming a
// minute of container time.
const MaxOps = 64

// MaxKeys bounds the key space. Three keys is enough for a per-key ordering
// comparison to be distinguishable from a global one, and few enough that
// every key gets several messages in a script of MaxOps.
const MaxKeys = 4

// MaxReceive bounds one OpReceive.
const MaxReceive = 8

// DecodeScript decodes a script from fuzz bytes. It is total: any input
// decodes to some script, and an exhausted reader yields the ops decoded so
// far.
func DecodeScript(data []byte) Script {
	r := fuzzutil.NewReader(data)
	s := Script{Keys: r.Int(1, MaxKeys)}
	for len(s.Ops) < MaxOps && !r.Exhausted() {
		op := Op{}
		switch {
		case r.Int(0, 3) == 0:
			op.Kind, op.N = OpReceive, r.Int(1, MaxReceive)
		default:
			op.Kind = OpPublish
			op.Key = r.Int(0, s.Keys-1)
			op.Action = Action(r.Int(int(ActionAck), int(ActionDrop)))
		}
		s.Ops = append(s.Ops, op)
	}
	return s.Normalise()
}

// Normalise makes a script worth running.
//
// A script the fuzzer produced can be all receives and no publishes, in which
// case the run compares nothing and the relation reports [KindVacuous]. Rather
// than let the fuzzer spend most of its inputs on vacuous runs, a script with
// no publish gets one per key, and every script ends with a receive so that at
// least one delivery is observed before the drain.
func (s Script) Normalise() Script {
	if s.Keys < 1 {
		s.Keys = 1
	}
	if s.Keys > MaxKeys {
		s.Keys = MaxKeys
	}
	pubs := 0
	for i := range s.Ops {
		if s.Ops[i].Kind == OpPublish {
			pubs++
			if !s.Ops[i].Action.valid() {
				s.Ops[i].Action = ActionAck
			}
		}
		if s.Ops[i].Kind == OpPublish && s.Ops[i].Key >= s.Keys {
			s.Ops[i].Key = s.Keys - 1
		}
	}
	if pubs == 0 {
		head := make([]Op, 0, s.Keys+len(s.Ops))
		for k := range s.Keys {
			head = append(head, Op{Kind: OpPublish, Key: k, Action: ActionAck})
		}
		s.Ops = append(head, s.Ops...)
	}
	if n := len(s.Ops); n == 0 || s.Ops[n-1].Kind != OpReceive {
		s.Ops = append(s.Ops, Op{Kind: OpReceive, N: MaxReceive})
	}
	// The cap is applied LAST and the trailing receive is then re-imposed,
	// because appending it to a script already at MaxOps produced 65 ops
	// that decoded back to 64 — so WriteScript's round trip changed the
	// script, and a committed seed would not have been the script it named.
	if len(s.Ops) > MaxOps {
		s.Ops = s.Ops[:MaxOps]
		s.Ops[MaxOps-1] = Op{Kind: OpReceive, N: MaxReceive}
	}
	return s
}

func (a Action) valid() bool { return a >= ActionAck && a <= ActionDrop }

// WriteScript encodes s back into fuzz bytes, so that a script found
// interesting by hand can be committed as a seed.
func WriteScript(s Script) []byte {
	w := fuzzutil.NewWriter()
	w.Int(1, MaxKeys, s.Keys)
	for _, op := range s.Ops {
		switch op.Kind {
		case OpPublish:
			w.Int(0, 3, 1)
			w.Int(0, s.Keys-1, op.Key)
			w.Int(int(ActionAck), int(ActionDrop), int(op.Action))
		case OpReceive:
			w.Int(0, 3, 0)
			w.Int(1, MaxReceive, op.N)
		}
	}
	return w.Data()
}

// Features is what both arms can do. A script is restricted to the
// INTERSECTION of the two arms' features before either runs it, because an
// action one arm cannot perform would otherwise make the two arms run
// different scripts — which the relation would report as a broker defect.
type Features struct {
	// Nack is whether a nack asking for redelivery is supported.
	Nack bool
	// Drop is whether a nack that discards the message is supported.
	Drop bool
}

// FeaturesOf reads the features from a capability table.
func FeaturesOf(caps mq.Capabilities) Features {
	return Features{
		Nack: caps.Has(mq.FeatureNack),
		// Discarding a message without redelivery is the nack action
		// The specification calls NackDrop. No capability names it on its own, so it
		// is read from the same claim as the nack it is an action of:
		// a driver that cannot nack at all cannot drop either.
		Drop: caps.Has(mq.FeatureNack),
	}
}

// Intersect is the features both arms have.
func Intersect(a, b Features) Features {
	return Features{Nack: a.Nack && b.Nack, Drop: a.Drop && b.Drop}
}

// Restrict rewrites the actions neither arm can perform into an ack, which
// every arm can. It returns the restricted script and how many it rewrote.
func (s Script) Restrict(f Features) (Script, int) {
	out := Script{Keys: s.Keys, Ops: make([]Op, len(s.Ops))}
	copy(out.Ops, s.Ops)
	n := 0
	for i := range out.Ops {
		if out.Ops[i].Kind != OpPublish {
			continue
		}
		switch out.Ops[i].Action {
		case ActionAck:
			// Nothing to restrict: every arm can ack.
		case ActionNackThenAck:
			if !f.Nack {
				out.Ops[i].Action, n = ActionAck, n+1
			}
		case ActionDrop:
			if !f.Drop {
				out.Ops[i].Action, n = ActionAck, n+1
			}
		}
	}
	return out, n
}

// Counts is how many of each op and each action the script holds, for a log
// line: a run whose script published twice and received once has not tested
// much, and the only way a reader can tell is if the run says so.
func (s Script) Counts() (ops map[OpKind]int, actions map[Action]int) {
	ops, actions = map[OpKind]int{}, map[Action]int{}
	for _, op := range s.Ops {
		ops[op.Kind]++
		if op.Kind == OpPublish {
			actions[op.Action]++
		}
	}
	return ops, actions
}

// String renders the script, which is what a failure message carries: a
// divergence with no script beside it cannot be reproduced.
func (s Script) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "keys=%d ops=%d:", s.Keys, len(s.Ops))
	for _, op := range s.Ops {
		b.WriteString(" ")
		b.WriteString(op.String())
	}
	return b.String()
}
