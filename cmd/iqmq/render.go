package main

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
)

// renderOptions are how `sub` and `tail` print a delivery.
//
// Headers are OFF by default and the body is ON. the security checks says
// bodies and headers are never logged BY DEFAULT, and the two halves of that
// rule land differently on a tool whose whole purpose is to show you a
// message: printing the body is what the caller invoked the command for, and
// printing every header is not -- a bearer token in an "authorization" header
// is exactly the case the specification is about, and it should take a flag.
type renderOptions struct {
	// json selects the machine-readable form.
	json bool
	// headers includes the message headers.
	headers bool
	// body includes the message body.
	body bool
	// base64 renders the body as base64 rather than as bytes. The JSON form
	// always encodes it, because encoding/json renders a []byte that way and
	// a body that is not valid UTF-8 has no other honest JSON form.
	base64 bool
	// meta includes the broker metadata DeliveryInfo carries.
	meta bool
}

// deliveryJSON is the machine-readable form of one delivery.
type deliveryJSON struct {
	N            int               `json:"n"`
	Topic        string            `json:"topic,omitempty"`
	Subscription string            `json:"subscription,omitempty"`
	Key          []byte            `json:"key,omitempty"`
	ID           string            `json:"id,omitempty"`
	Attempt      int               `json:"attempt,omitempty"`
	Redelivered  bool              `json:"redelivered,omitempty"`
	Bytes        int               `json:"bytes"`
	Headers      map[string]string `json:"headers,omitempty"`
	Body         []byte            `json:"body,omitempty"`
	Partition    optionalInt       `json:"partition,omitzero"`
	Offset       optionalInt       `json:"offset,omitzero"`
	Sequence     optionalUint      `json:"sequence,omitzero"`
	ReceivedAt   string            `json:"received_at,omitempty"`
	LeaseExpires string            `json:"lease_expires,omitempty"`
	Settlement   string            `json:"settlement,omitempty"`
}

// optionalInt is a number a row carries only when the driver reported one. A
// zero partition or offset is real, so the zero value cannot stand for absent.
type optionalInt struct {
	value int64
	valid bool
}

var _ json.Marshaler = optionalInt{}

// IsZero is what omitzero asks, so an absent number leaves the row.
func (o optionalInt) IsZero() bool { return !o.valid }

// MarshalJSON writes the number itself rather than the struct.
func (o optionalInt) MarshalJSON() ([]byte, error) { return strconv.AppendInt(nil, o.value, 10), nil }

// optionalUint is optionalInt for a sequence number, which is unsigned.
type optionalUint struct {
	value uint64
	valid bool
}

var _ json.Marshaler = optionalUint{}

// IsZero is what omitzero asks, so an absent number leaves the row.
func (o optionalUint) IsZero() bool { return !o.valid }

// MarshalJSON writes the number itself rather than the struct.
func (o optionalUint) MarshalJSON() ([]byte, error) { return strconv.AppendUint(nil, o.value, 10), nil }

// captured is one delivery, copied out of the consumer's storage before it is
// settled.
//
// The copy is deliberate. The specification borrows a received body until the delivery is
// settled, and this tool prints the settlement outcome ON the delivery's own
// line -- an acknowledged message and one abandoned at shutdown have to be
// distinguishable, and a line that showed only the payload cannot do it. So
// the message is cloned, then settled, then printed, which costs a copy per
// message and is well within what a CLI can afford.
type captured struct {
	msg     *mq.Message
	info    mq.DeliveryInfo
	attempt int
}

// capture copies a delivery out of the consumer's storage.
func capture(d *mq.Delivery) captured {
	return captured{msg: d.CloneMessage(), info: d.Info(), attempt: d.Attempt}
}

// renderCaptured writes one captured delivery, with what the command settled
// it as.
func renderCaptured(w io.Writer, n int, c *captured, settlement string, o *renderOptions) {
	d, info := c.msg, c.info
	if o.json {
		row := deliveryJSON{
			N: n, Topic: info.Topic, Subscription: info.Subscription,
			Key: d.Key, ID: d.ID, Attempt: c.attempt, Redelivered: info.Redelivered,
			Bytes: len(d.Body), Settlement: settlement,
		}
		if info.Topic == "" {
			row.Topic = d.Topic
		}
		if o.headers && d.Headers.Len() > 0 {
			row.Headers = make(map[string]string, d.Headers.Len())
			for k, v := range d.Headers.All() {
				// A repeated key is joined rather than dropped: the specification keeps
				// duplicates, and a JSON object cannot.
				if prev, ok := row.Headers[k]; ok {
					row.Headers[k] = prev + "," + string(v)
					continue
				}
				row.Headers[k] = string(v)
			}
		}
		if o.body {
			row.Body = d.Body
		}
		if o.meta {
			if info.HasPartition {
				row.Partition = optionalInt{value: int64(info.Partition), valid: true}
			}
			if info.HasOffset {
				row.Offset = optionalInt{value: info.Offset, valid: true}
			}
			if info.HasSequence {
				row.Sequence = optionalUint{value: info.Sequence, valid: true}
			}
			if !info.ReceivedAt.IsZero() {
				row.ReceivedAt = info.ReceivedAt.Format(time.RFC3339Nano)
			}
			if !info.LeaseExpires.IsZero() {
				row.LeaseExpires = info.LeaseExpires.Format(time.RFC3339Nano)
			}
		}
		enc := json.NewEncoder(w)
		if err := enc.Encode(row); err != nil {
			_, _ = io.WriteString(w, "{\"error\":"+quote(err.Error())+"}\n")
		}
		return
	}

	var b strings.Builder
	b.WriteString("#")
	b.WriteString(itoa(n))
	topic := info.Topic
	if topic == "" {
		topic = d.Topic
	}
	if topic != "" {
		b.WriteString(" topic=")
		b.WriteString(topic)
	}
	if len(d.Key) > 0 {
		b.WriteString(" key=")
		b.Write(d.Key)
	}
	if d.ID != "" {
		b.WriteString(" id=")
		b.WriteString(d.ID)
	}
	if c.attempt > 0 {
		b.WriteString(" attempt=")
		b.WriteString(itoa(c.attempt))
	}
	if info.Redelivered {
		b.WriteString(" redelivered")
	}
	b.WriteString(" bytes=")
	b.WriteString(itoa(len(d.Body)))
	if o.meta {
		if info.HasPartition {
			b.WriteString(" partition=")
			b.WriteString(itoa(int(info.Partition)))
		}
		if info.HasOffset {
			b.WriteString(" offset=")
			b.WriteString(i64toa(info.Offset))
		}
		if info.HasSequence {
			b.WriteString(" sequence=")
			b.WriteString(i64toa(int64(info.Sequence)))
		}
		if !info.LeaseExpires.IsZero() {
			b.WriteString(" lease-expires=")
			b.WriteString(info.LeaseExpires.Format(time.RFC3339))
		}
	}
	if settlement != "" {
		b.WriteString(" settlement=")
		b.WriteString(settlement)
	}
	b.WriteByte('\n')
	if o.headers {
		for k, v := range d.Headers.All() {
			b.WriteString("  header ")
			b.WriteString(k)
			b.WriteString("=")
			b.Write(v)
			b.WriteByte('\n')
		}
	}
	if o.body {
		b.WriteString("  ")
		switch {
		case o.base64:
			b.WriteString(base64.StdEncoding.EncodeToString(d.Body))
		default:
			b.Write(d.Body)
		}
		b.WriteByte('\n')
	}
	_, _ = io.WriteString(w, b.String())
}

// supportLine renders one capability for a message that has to explain a
// refusal: the feature, the level, the scope and the condition a conditional
// support states.
func supportLine(caps mq.Capabilities, f mq.Feature) string {
	s := caps.Support(f)
	out := f.String() + "=" + s.Level.String()
	if s.Scope != mq.ScopeNone {
		out += " scope=" + s.Scope.String()
	}
	if s.Note != "" {
		out += " (" + s.Note + ")"
	}
	return out
}
