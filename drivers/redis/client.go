package redis

import (
	"context"

	"github.com/redis/rueidis"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/streams"
)

// commander is [streams.Commander] over rueidis.
//
// Everything interesting in here is one of two things: how a [streams.Cmd]
// becomes a rueidis command, and how a rueidis reply becomes a
// [streams.Value].
//
// It deliberately does NOT implement [streams.ArgAliaser], so every message
// body is copied into the command as a string. The reason is rueidis's
// pipelining and not a policy: `pipe.Do` puts a command on a queue for a
// background writer, and when the context is cancelled it hands the reply
// channel to a goroutine and returns `ctx.Err()` while the command is still
// queued -- `_backgroundWrite` then writes that command's tokens from the
// other goroutine, after Publish has returned. the message contract lets the
// caller reuse its buffer the instant Publish returns, so an aliased body
// there is not a failed publication but a CORRUPT entry in the stream, with
// nothing to report it. `rueidis.BinaryString` -- which is
// `unsafe.String(unsafe.SliceData(bs), len(bs))` and is what the raw-client
// baseline uses -- has the same exposure and is measured against this driver
// anyway, so part of the breach on this profile is a guarantee the
// comparand does not offer.
type commander struct {
	client rueidis.Client
}

var (
	_ streams.Commander     = (*commander)(nil)
	_ streams.Pinger        = (*commander)(nil)
	_ streams.ServerInfo    = (*commander)(nil)
	_ streams.ClientLibrary = (*commander)(nil)
	_ pinner                = (*commander)(nil)
	_ nodeLister            = (*commander)(nil)
)

// ClientLibrary names the library behind this commander.
func (c *commander) ClientLibrary() (module, version string) {
	return ClientModule, rueidisVersion
}

// NewCommander returns a [streams.Commander] over a rueidis client.
//
// It is exported for the same reason [MapForTest] and [SweepForTest] are:
// `drivers/redis/goredis` holds its own adapter to this one's answers, and a
// comparison between one shipped adapter and a copy of the other written for
// the test would prove nothing about what ships. The returned commander OWNS
// the client: Close closes it.
func NewCommander(client rueidis.Client) (streams.Commander, error) {
	if client == nil {
		return nil, errNilClient
	}
	return &commander{client: client}, nil
}

// Nodes is one commander per node when the client is a cluster's, and nil
// otherwise. The commanders share this one's connections, so they are not
// closed: closing this commander closes them.
func (c *commander) Nodes(context.Context) ([]streams.Commander, error) {
	if c.client.Mode() != rueidis.ClientModeCluster {
		return nil, nil
	}
	nodes := c.client.Nodes()
	out := make([]streams.Commander, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, &commander{client: n})
	}
	return out, nil
}

// build turns a Cmd into a rueidis command.
//
// The key span is why [streams.Cmd] carries one. rueidis computes a cluster
// slot from the tokens passed to Keys and from nothing else, so a command
// whose keys went through Args would be routed by the first key rueidis
// happened to see — which for XREADGROUP is no key at all, because its keys
// sit between STREAMS and the identifiers. A command built without the span
// works on a standalone server and goes to the wrong node on a cluster, which
// is the worst possible place for that mistake to show up.
//
// Blocking is honoured too: rueidis routes a command marked Blocking to a
// separate connection pool, and a BLOCK on the shared pipelined connection
// stalls every command queued behind it for the whole block.
func (c *commander) build(cmd streams.Cmd) rueidis.Completed {
	b := c.client.B().Arbitrary(cmd.Tokens[0])
	keyStart := cmd.KeyStart
	keyEnd := keyStart + cmd.KeyCount
	for i := 1; i < len(cmd.Tokens); i++ {
		if cmd.KeyCount > 0 && i >= keyStart && i < keyEnd {
			b = b.Keys(cmd.Tokens[i])
			continue
		}
		// Text and not Tokens[i]: this commander does not implement
		// [streams.ArgAliaser], so nothing builds a blob-carrying command for
		// it -- and if something ever did, Text COPIES the blob's bytes where
		// Tokens[i] would send the empty placeholder and store an entry with
		// no body. The fallback costs one length check per token on a command
		// that has no blobs.
		b = b.Args(cmd.Text(i))
	}
	if cmd.Blocking {
		return b.Blocking()
	}
	return b.Build()
}

// Do sends one command.
func (c *commander) Do(ctx context.Context, cmd streams.Cmd) (streams.Value, error) {
	if !cmd.Valid() {
		return streams.Nil(), errBadCommand
	}
	return convert(c.client.Do(ctx, c.build(cmd)))
}

// DoMulti pipelines cmds and returns one result per command, in order.
//
// A pipeline is not a transaction: Redis executes each command independently
// and reports each outcome, which is exactly the per-item result the batch
// contract needs. A driver that collapsed the slice to the first error would
// report one delivery's outcome for another.
func (c *commander) DoMulti(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	if len(cmds) == 0 {
		return nil, nil
	}
	built, err := c.buildAll(cmds)
	if err != nil {
		return nil, err
	}
	return toResults(len(cmds), c.client.DoMulti(ctx, built...)), nil
}

// DoPinned is DoMulti on a dedicated connection.
//
// rueidis multiplexes commands from every goroutine over a few shared
// pipelines and picks one by slot, so neither two Do calls nor a DoMulti from
// one goroutine is promised a connection of its own; Dedicated is the call
// that is.
func (c *commander) DoPinned(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	if len(cmds) == 0 {
		return nil, nil
	}
	built, err := c.buildAll(cmds)
	if err != nil {
		return nil, err
	}
	var out []streams.Result
	err = c.client.Dedicated(func(dc rueidis.DedicatedClient) error {
		out = toResults(len(cmds), dc.DoMulti(ctx, built...))
		return nil
	})
	return out, err
}

// buildAll builds every command of a pipeline, refusing the pipeline when one
// of them is not a command.
func (c *commander) buildAll(cmds []streams.Cmd) ([]rueidis.Completed, error) {
	built := make([]rueidis.Completed, 0, len(cmds))
	for _, cmd := range cmds {
		if !cmd.Valid() {
			return nil, errBadCommand
		}
		built = append(built, c.build(cmd))
	}
	return built, nil
}

// toResults converts a pipeline's replies, one Result per command.
func toResults(n int, replies []rueidis.RedisResult) []streams.Result {
	out := make([]streams.Result, n)
	for i := range out {
		if i >= len(replies) {
			out[i] = streams.Result{Err: errShortPipeline}
			continue
		}
		v, err := convert(replies[i])
		out[i] = streams.Result{Value: v, Err: err}
	}
	return out
}

// Close releases the client.
func (c *commander) Close() error {
	c.client.Close()
	return nil
}

// Ping is the liveness check one round trip that consumes no message
// and creates no topology.
func (c *commander) Ping(ctx context.Context) error {
	_, err := c.Do(ctx, streams.Ping())
	return err
}

// ServerVersion reads the version out of INFO server.
//
// The capability table branches on it — WAITAOF needs 7.0, XAUTOCLAIM 6.2 —
// so a driver that could not ask would either claim what it may not have or
// claim nothing. Valkey reports its own version in redis_version for
// compatibility and its identity in server_name, so both are read.
func (c *commander) ServerVersion(ctx context.Context) (version, software string, err error) {
	v, err := c.Do(ctx, streams.InfoServer())
	if err != nil {
		return "", "", err
	}
	text, ok := v.Str()
	if !ok {
		return "", "", nil
	}
	version, software = streams.ParseServerInfo(text)
	return version, software, nil
}

// convert turns a rueidis reply into a [streams.Value].
//
// The two things it has to get right are what the whole command layer depends
// on. A Redis error reply becomes an error and not a Value, so a driver cannot
// read "NOGROUP" as data; and a RESP3 map becomes a FLAT array of alternating
// key and value, which is RESP2's shape and the one that preserves the order
// an entry's fields were written in. A layer that turned an entry's fields
// into a Go map would lose exactly the order the header indices depend on.
func convert(res rueidis.RedisResult) (streams.Value, error) {
	if err := res.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return streams.Nil(), nil
		}
		return streams.Nil(), err
	}
	msg, err := res.ToMessage()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return streams.Nil(), nil
		}
		return streams.Nil(), err
	}
	return convertMessage(&msg)
}

// convertMessage is convert's recursive half.
func convertMessage(msg *rueidis.RedisMessage) (streams.Value, error) {
	switch {
	case msg.IsNil():
		return streams.Nil(), nil
	case msg.IsInt64():
		n, err := msg.AsInt64()
		if err != nil {
			return streams.Nil(), err
		}
		return streams.Int(n), nil
	case msg.IsBool():
		b, err := msg.AsBool()
		if err != nil {
			return streams.Nil(), err
		}
		return streams.Bool(b), nil
	case msg.IsMap():
		m, err := msg.ToMap()
		if err != nil {
			return streams.Nil(), err
		}
		// Flattened in the key order the map iteration gives, which is not
		// stable — and that is acceptable for the ONE place a map reply
		// reaches this driver (XINFO's field/value pairs, read by name).
		// Nothing whose order matters arrives as a map: an entry's fields are
		// an array in every RESP version.
		out := make([]streams.Value, 0, 2*len(m))
		for k, v := range m {
			child, cerr := convertMessage(&v)
			if cerr != nil {
				return streams.Nil(), cerr
			}
			out = append(out, streams.Str(k), child)
		}
		return streams.ArrOf(out), nil
	case msg.IsArray():
		items, err := msg.ToArray()
		if err != nil {
			return streams.Nil(), err
		}
		out := make([]streams.Value, 0, len(items))
		for i := range items {
			child, cerr := convertMessage(&items[i])
			if cerr != nil {
				return streams.Nil(), cerr
			}
			out = append(out, child)
		}
		return streams.ArrOf(out), nil
	case msg.IsFloat64():
		// A float reply is rendered as its string form: nothing this driver
		// sends returns one, and reading it as an integer would round.
		s, err := msg.ToString()
		if err != nil {
			return streams.Nil(), err
		}
		return streams.Str(s), nil
	}
	s, err := msg.ToString()
	if err != nil {
		return streams.Nil(), err
	}
	return streams.Str(s), nil
}

// The commander's own refusals.
var (
	errBadCommand = errcode.New(errcode.InvalidArgument,
		"iqmq/redis: the command is empty or its key span is outside its tokens")
	errShortPipeline = errcode.New(errcode.CorruptMessage,
		"iqmq/redis: the client returned fewer replies than the pipeline had commands")
)
