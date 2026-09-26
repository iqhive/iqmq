package respsrv

import (
	"bufio"
	"strconv"
	"strings"

	"github.com/iqhive/iqmq/internal/streams"
)

// writer renders replies in the protocol version this connection negotiated.
//
// The version is per connection and not per server, which is the protocol's
// own rule: a client sends HELLO 3 and everything after it is RESP3, and a
// client that never sends HELLO gets RESP2 for ever. Both Redis client
// libraries this repository ships behind [streams.Commander] speak RESP3 by
// default and both can be configured down to RESP2, so both paths are
// reachable and both are exercised.
type writer struct {
	bw    *bufio.Writer
	proto int
	// err is the first write failure. Every method below is a no-op once it
	// is set, so a connection whose peer went away mid-reply unwinds at the
	// end of the reply rather than at every byte of it.
	err error
}

// str appends a string.
func (w *writer) str(s string) {
	if w.err == nil {
		_, w.err = w.bw.WriteString(s)
	}
}

// byte appends one byte.
func (w *writer) byte(b byte) {
	if w.err == nil {
		w.err = w.bw.WriteByte(b)
	}
}

// simple writes a simple-string reply ("+OK").
func (w *writer) simple(s string) {
	w.byte('+')
	w.str(oneLine(s))
	w.crlf()
}

// integer writes an integer reply.
func (w *writer) integer(n int64) {
	w.byte(':')
	w.str(strconv.FormatInt(n, 10))
	w.crlf()
}

// bulk writes a bulk-string reply. It is binary-safe: the length is what the
// peer reads by, so a value with a CRLF or a NUL in it survives, which is
// exactly what an entry's body needs.
func (w *writer) bulk(s string) {
	w.byte('$')
	w.str(strconv.Itoa(len(s)))
	w.crlf()
	w.str(s)
	w.crlf()
}

// null writes the version-appropriate null.
func (w *writer) null() {
	if w.proto >= 3 {
		w.str("_\r\n")
		return
	}
	// RESP2 has two nulls -- the null bulk string and the null array -- and
	// every client library maps both to the same "no value". The bulk form is
	// written for both, because the alternative is to guess from the command
	// which one a real server would have sent.
	w.str("$-1\r\n")
}

// boolean writes the version-appropriate boolean.
func (w *writer) boolean(b bool) {
	if w.proto >= 3 {
		if b {
			w.str("#t\r\n")
		} else {
			w.str("#f\r\n")
		}
		return
	}
	if b {
		w.integer(1)
		return
	}
	w.integer(0)
}

// errorReply writes an error reply.
//
// The condition word matters more than the text. Redis reports almost
// everything as an error reply whose first word is the condition -- NOGROUP,
// BUSYGROUP, NOSCRIPT, CROSSSLOT, WRONGTYPE -- and every driver's error table
// matches on exactly those prefixes, so an error that arrived here already
// shaped like a server's is passed through unchanged and anything else is
// given the generic ERR the protocol requires. Without the second half a Go
// error such as "context canceled" would be written as a reply whose first
// word is a condition no client recognises.
func (w *writer) errorReply(err error) {
	w.byte('-')
	w.str(oneLine(errorText(err)))
	w.crlf()
}

// errorText is err's message with a condition word guaranteed.
func errorText(err error) string {
	text := err.Error()
	word, _, _ := strings.Cut(text, " ")
	if isCondition(word) {
		return text
	}
	return "ERR " + text
}

// isCondition reports whether s looks like a server's condition word: three
// or more characters, all upper-case ASCII letters.
func isCondition(s string) bool {
	if len(s) < 3 {
		return false
	}
	for i := range len(s) {
		if s[i] < 'A' || s[i] > 'Z' {
			return false
		}
	}
	return true
}

// oneLine removes the two bytes a simple reply cannot carry.
//
// A simple string and an error reply are terminated by CRLF and carry no
// length, so a CR or an LF inside one is a second reply as far as the peer is
// concerned -- a reply-splitting injection, and with a fake server driving a
// real client library the desynchronised connection that follows is the
// hardest possible thing to debug.
func oneLine(s string) string {
	if !strings.ContainsAny(s, "\r\n") {
		return s
	}
	return strings.Map(func(r rune) rune {
		if r == '\r' || r == '\n' {
			return ' '
		}
		return r
	}, s)
}

// value writes one [streams.Value].
func (w *writer) value(v streams.Value) {
	switch v.Kind() {
	case streams.KindNil:
		w.null()
	case streams.KindInt:
		n, _ := v.Int64()
		w.integer(n)
	case streams.KindBool:
		b, _ := v.Bool()
		w.boolean(b)
	case streams.KindString:
		s, _ := v.Str()
		w.bulk(s)
	case streams.KindArray:
		items, _ := v.Arr()
		w.array(items)
	default:
		// A Value whose kind is not one of the five cannot be rendered, and
		// an unrenderable reply has to become an error reply rather than
		// nothing at all: a connection that wrote no reply for a command is
		// desynchronised for the rest of its life.
		w.str("-ERR respsrv: unrenderable reply kind\r\n")
	}
}

// array writes an array reply.
func (w *writer) array(items []streams.Value) {
	w.byte('*')
	w.str(strconv.Itoa(len(items)))
	w.crlf()
	for _, item := range items {
		w.value(item)
	}
}

// mapReply writes a flat key/value list as RESP3's map, or as RESP2's flat
// array.
//
// HELLO is the only command here whose reply is a map, and it is the one that
// has to be a map: both client libraries read HELLO's reply with a map
// reader, so a RESP3 connection handed an array for it fails to initialise
// with a protocol error rather than with anything a reader would recognise.
func (w *writer) mapReply(pairs []streams.Value) {
	if w.proto >= 3 {
		w.byte('%')
		w.str(strconv.Itoa(len(pairs) / 2))
	} else {
		w.byte('*')
		w.str(strconv.Itoa(len(pairs)))
	}
	w.crlf()
	for _, p := range pairs {
		w.value(p)
	}
}

func (w *writer) crlf() { w.str("\r\n") }
