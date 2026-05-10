package respsrv

import (
	"context"
	"strings"

	"bitbucket.org/iqhive/iqmq/internal/streams"
)

// dispatch answers one command. done reports that the connection should be
// closed after the reply (QUIT).
//
// The split between what this answers itself and what it delegates is the
// whole design. A client library will not execute a single application command
// until its connection handshake has succeeded, and the handshake is not part
// of what a [streams.Commander] knows anything about: HELLO negotiates the
// protocol version, CLIENT SETINFO announces the library's name, SELECT picks
// the database, AUTH presents credentials. Those are answered here. Everything
// else -- every command the drivers actually send -- goes to the Commander
// unchanged, so that what a test or a benchmark exercises over this server is
// the client library and the adapter above it and nothing of this package's
// own invention.
func (c *session) dispatch(ctx context.Context, tokens []string) (done bool, err error) {
	if len(tokens) == 0 {
		c.w.errorReply(&ProtocolError{Want: "a command", Got: ""})
		return false, c.w.err
	}
	verb := strings.ToUpper(tokens[0])
	args := tokens[1:]
	switch verb {
	case "HELLO":
		c.hello(args)
	case "AUTH":
		// Credentials are accepted without being checked, and the reason is
		// what this server is for: refusing them would test this package's
		// idea of an ACL, not the client library's handshake. A test that
		// needs a refusal injects one through the Commander's own fault
		// table.
		c.w.simple("OK")
	case "SELECT":
		c.w.simple("OK")
	case "CLIENT":
		c.client(args)
	case "COMMAND":
		// An empty listing. A client that asked for the command table gets
		// "I have nothing to declare" rather than an error, because an error
		// here is fatal to some libraries' initialisation.
		c.w.array(nil)
	case "RESET":
		c.w.simple("RESET")
	case "QUIT":
		c.w.simple("OK")
		return true, c.w.err
	default:
		c.forward(ctx, tokens)
	}
	return false, c.w.err
}

// forward hands the command to the Commander and writes its answer.
//
// The key span is deliberately empty. [streams.Cmd]'s span exists so that a
// CLUSTER client can hash the right tokens, which is a question about the
// client library and not about the server: a server is handed the tokens and
// knows its own key positions. Nothing under this seam reads the span.
func (c *session) forward(ctx context.Context, tokens []string) {
	v, err := c.srv.cmder.Do(ctx, streams.Cmd{Tokens: tokens})
	if err != nil {
		c.w.errorReply(err)
		return
	}
	c.w.value(v)
}

// hello negotiates the protocol version.
//
// `HELLO` with no version reports the connection's current one; `HELLO 2` and
// `HELLO 3` switch. A version this server does not speak is refused with
// NOPROTO, which is the real server's answer and the one a client library
// knows how to fall back from.
func (c *session) hello(args []string) {
	proto := c.w.proto
	if len(args) > 0 {
		switch args[0] {
		case "2":
			proto = 2
		case "3":
			proto = 3
		default:
			c.w.str("-NOPROTO unsupported protocol version\r\n")
			return
		}
	}
	// The reply is rendered in the NEW version, which is the protocol's own
	// rule and easy to get backwards: a client that sent HELLO 3 reads the
	// reply to it with a RESP3 reader, so a RESP2 array here desynchronises
	// the connection on its very first command.
	c.w.proto = proto
	c.w.mapReply([]streams.Value{
		streams.Str("server"), streams.Str(c.srv.opts.Software),
		streams.Str("version"), streams.Str(c.srv.opts.Version),
		streams.Str("proto"), streams.Int(int64(proto)),
		streams.Str("id"), streams.Int(1),
		streams.Str("mode"), streams.Str("standalone"),
		streams.Str("role"), streams.Str("master"),
		streams.Str("modules"), streams.ArrOf(nil),
	})
}

// client answers the CLIENT subcommands a client library sends while setting
// a connection up.
//
// They are answered rather than delegated because none of them is a question
// about streams, and because a client library treats a failure of one of them
// as a failed connection: go-redis sends CLIENT SETINFO with its own name and
// version on every new connection, and rueidis sends it too.
func (c *session) client(args []string) {
	switch upper(args, 0) {
	case "ID":
		c.w.integer(1)
	case "INFO":
		c.w.bulk("id=1 name= lib-name= lib-ver=")
	case "GETNAME":
		c.w.null()
	default:
		// SETINFO, SETNAME, TRACKING, NO-EVICT, NO-TOUCH, UNPAUSE, REPLY ON.
		// Every one of them is bookkeeping about the connection, and a fake
		// server has nothing to record; OK is what a real one answers.
		c.w.simple("OK")
	}
}

// upper is args[i] upper-cased, or "" when there is no such argument.
//
// It exists for the same reason `fakestreams` has one: an index computed from
// an argument the peer may not have sent is a panic on the path where the peer
// is malformed, and a fuzz target finds it in under a second.
func upper(args []string, i int) string {
	if i < 0 || i >= len(args) {
		return ""
	}
	return strings.ToUpper(args[i])
}
