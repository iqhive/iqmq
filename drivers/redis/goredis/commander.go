package goredis

import (
	"context"
	"errors"
	"math"
	"math/big"
	"strconv"
	"sync"

	redis "github.com/redis/go-redis/v9"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/streams"
)

// ClientModule is the client library this driver wraps, and ClientVersion its
// version. They are constants rather than a build-info lookup because the specification's
// `DriverInfo` is answered without I/O and because a vendored build reports no
// module version at all.
const (
	ClientModule  = "github.com/redis/go-redis/v9"
	ClientVersion = "v9.23.0"
)

// commander is [streams.Commander] over go-redis.
//
// It is the whole of what this module adds. Two functions in it are worth
// reading before anything else, because they are the only places a client
// library can silently mean something different from its sibling: [build],
// which turns a [streams.Cmd] into a go-redis command, and [convert], which
// turns go-redis's reply into a [streams.Value].
type commander struct {
	client redis.UniversalClient
	// own says whether Close closes the client, and it is the CALLER's
	// statement rather than an inference: [OpenBackend] dialled the client
	// and owns it, [BackendWithClient] is told which it is, and
	// [NewCommander] owns because its sibling in `drivers/redis` does.
	// Guessing from how the commander was built is what the specification forbids -- a
	// driver that always closed would tear down a client the application
	// still uses, and one that never closed would leak the one it dialled.
	own bool
}

var (
	_ streams.Commander     = (*commander)(nil)
	_ streams.Pinger        = (*commander)(nil)
	_ streams.ServerInfo    = (*commander)(nil)
	_ streams.ClientLibrary = (*commander)(nil)
	_ streams.ArgAliaser    = (*commander)(nil)
)

// NewCommander returns a [streams.Commander] over a go-redis client, for a
// caller composing `internal/streams` directly rather than going through the
// driver.
//
// The returned commander OWNS the client: Close closes it. That is
// `drivers/redis.NewCommander`'s contract too, and the two have to agree --
// two sibling modules whose same-named constructor took opposite ownership
// would be a trap, and the wrapper the caller writes to paper over the
// difference is the smell that says so. A caller that must keep the client
// goes through [BackendWithClient] with [driverkit.ClientBorrowed], which is
// where the specification puts the question.
func NewCommander(client redis.UniversalClient) (streams.Commander, error) {
	if client == nil {
		return nil, errNilClient
	}
	return &commander{client: client, own: true}, nil
}

// ClientLibrary names the library behind this commander.
func (c *commander) ClientLibrary() (module, version string) {
	return ClientModule, ClientVersion
}

// AliasArgs accepts a command whose body token is the caller's own []byte.
//
// The claim [streams.ArgAliaser] asks for is "every byte of every token is
// read before Do or DoMulti returns, and none afterwards", and go-redis meets
// it by writing in the CALLING goroutine. `baseClient._process` takes a
// connection, writes the command inside `cn.WithWriter` and reads the reply
// inside `cn.WithReader`, both synchronously; `Pipeline.Exec` does the same
// for the whole batch. The library owns no writer goroutine and keeps no queue
// of commands to write later, so there is no path on which a token is read
// after the call that supplied it has returned.
//
// The three ways a retained argument usually escapes are all closed here:
//
//   - a RETRY (`processWithRetry`, up to `MaxRetries`) re-writes the command,
//     but inside the same call, so the caller has not got its buffer back;
//   - a RECONNECTION dials and re-writes inside the same call for the same
//     reason;
//   - a CANCELLED context aborts the write or the read and removes the
//     connection, and leaves nothing queued -- which is exactly where rueidis
//     differs, and why `drivers/redis` answers this question by not
//     implementing the interface at all.
//
// What is NOT covered is a go-redis hook (`redis.Hook`) that retains the
// `Cmder` past its own call and reads its arguments later. That is outside
// anything this driver can see: a caller who installs one through
// [OpenWithClient] and stores commands for later must copy what it stores,
// which is the same obligation any `Cmder` argument carries. It is stated in
// this module's README, under "the message body"; `docs/drivers/goredis.md`
// does not carry it yet and that is recorded as a follow-up.
func (c *commander) AliasArgs() bool { return true }

// Nodes is one commander per master when the client is a cluster's, and nil
// otherwise. Each one borrows the cluster client's own per-node client, so it
// never closes it.
func (c *commander) Nodes(ctx context.Context) ([]streams.Commander, error) {
	cl, ok := c.client.(*redis.ClusterClient)
	if !ok {
		return nil, nil
	}
	var (
		mu  sync.Mutex
		out []streams.Commander
	)
	// ForEachMaster calls fn on every master concurrently.
	err := cl.ForEachMaster(ctx, func(_ context.Context, node *redis.Client) error {
		mu.Lock()
		out = append(out, &commander{client: node})
		mu.Unlock()
		return nil
	})
	return out, err
}

// build turns a Cmd into a go-redis command.
//
// The key span is why [streams.Cmd] carries one, and go-redis needs it for the
// same reason rueidis does and asks for it differently. A cluster client has
// to hash a key to pick a node, and go-redis finds the key by POSITION: it
// looks the position up in the server's own command table, which a managed
// Redis may not serve and which knows nothing about a command sent through
// `Do`. `SetFirstKeyPos` is how a caller states it, and a command sent without
// it is routed by `hashtag.RandomSlot()` -- a RANDOM node, which on a
// standalone server is correct by accident and on a cluster is the wrong node
// for every command this driver sends.
//
// Only the first key's position is stated because that is all go-redis's
// routing reads, and it is enough: every key of every command this driver
// builds carries the topic's hash tag, so all of a command's keys are on one
// slot and the first one names it (see [streams.Keys]).
func build(ctx context.Context, cmd streams.Cmd) (*redis.Cmd, error) {
	args := make([]any, len(cmd.Tokens))
	for i, tok := range cmd.Tokens {
		args[i] = tok
	}
	if len(cmd.Blobs) != 0 {
		aliasArgs(args, cmd.Blobs)
	}
	out := redis.NewCmd(ctx, args...)
	if cmd.KeyCount > 0 {
		if cmd.KeyStart > math.MaxInt8 {
			// go-redis carries the position in an int8. No command this
			// driver builds comes close -- the furthest is EVALSHA's key at
			// token 3 and XREADGROUP's at token 10 -- and the alternative to
			// refusing is a command routed to a random node, so the bound is
			// a refusal rather than a clamp.
			return nil, errKeyPositionTooFar
		}
		out.SetFirstKeyPos(int8(cmd.KeyStart))
	}
	return out, nil
}

// aliasArgs replaces the placeholder tokens of a blob-carrying command with
// the caller's own bytes.
//
// It is a second pass over the arguments rather than a branch inside the
// loop, and the shape is deliberate twice over. The loop above stays exactly
// what it was, so every command that carries no blob -- every settlement,
// every fetch, every topology command, and every publication on a client that
// cannot be trusted with the caller's bytes -- costs one length check more
// than before and nothing else. And a blob's placeholder is the EMPTY string,
// which `runtime.convTstring` answers from a static without allocating, so
// boxing it and then overwriting it is free.
//
// go-redis writes a []byte argument with `proto.Writer.bytes`, straight into
// the connection's write buffer with no string conversion anywhere. That is
// the per-message body copy the batched budget was paying for; see
// [commander.AliasArgs] for why this client may be trusted with it.
func aliasArgs(args []any, blobs []streams.Blob) {
	for _, b := range blobs {
		args[b.At] = b.Bytes
	}
}

// Do sends one command.
func (c *commander) Do(ctx context.Context, cmd streams.Cmd) (streams.Value, error) {
	if !cmd.Valid() {
		return streams.Nil(), errBadCommand
	}
	built, err := build(ctx, cmd)
	if err != nil {
		return streams.Nil(), err
	}
	// Process rather than Do, because Do builds its own command and there is
	// no way to tell that one a key position. Process's error is discarded
	// because it is the COMMAND's error: go-redis sets it on the command it
	// was given and returns the same value, and reading it from the command
	// is what keeps the null reply ([redis.Nil]) on the one path that turns
	// it into a value.
	_ = c.client.Process(ctx, built)
	return convert(built.Result())
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
	return execPipeline(ctx, c.client.Pipeline(), cmds)
}

// DoPinned is DoMulti on one connection.
//
// A pipeline of a plain client already runs on one pooled connection, but
// nothing promises that of a cluster client, which routes a keyless command
// such as WAITAOF to any node. So the connection is taken explicitly: the
// client's own, or the master's of the first key the commands name, and a
// client of another shape gets the pipeline it already has.
func (c *commander) DoPinned(ctx context.Context, cmds []streams.Cmd) ([]streams.Result, error) {
	if len(cmds) == 0 {
		return nil, nil
	}
	switch cl := c.client.(type) {
	case *redis.Client:
		return pinnedPipeline(ctx, cl, cmds)
	case *redis.ClusterClient:
		node, err := cl.MasterForKey(ctx, firstKey(cmds))
		if err != nil {
			return nil, err
		}
		return pinnedPipeline(ctx, node, cmds)
	default:
		return c.DoMulti(ctx, cmds)
	}
}

// pinnedPipeline sends cmds as one pipeline on one of client's connections.
func pinnedPipeline(ctx context.Context, client *redis.Client, cmds []streams.Cmd) ([]streams.Result, error) {
	conn := client.Conn()
	defer func() { _ = conn.Close() }()
	return execPipeline(ctx, conn.Pipeline(), cmds)
}

// firstKey is the first key any of cmds names, or "" when none does.
func firstKey(cmds []streams.Cmd) string {
	for _, cmd := range cmds {
		if cmd.KeyCount > 0 && cmd.KeyStart < len(cmd.Tokens) {
			return cmd.Tokens[cmd.KeyStart]
		}
	}
	return ""
}

// execPipeline sends cmds through pipe and returns one result per command.
func execPipeline(ctx context.Context, pipe redis.Pipeliner, cmds []streams.Cmd) ([]streams.Result, error) {
	built := make([]*redis.Cmd, len(cmds))
	for i, cmd := range cmds {
		if !cmd.Valid() {
			return nil, errBadCommand
		}
		b, err := build(ctx, cmd)
		if err != nil {
			return nil, err
		}
		built[i] = b
		if perr := pipe.Process(ctx, b); perr != nil {
			return nil, perr
		}
	}
	// Exec's own error is the FIRST failure among the commands, so it must
	// not be attributed to the others: a pipeline of ten XACKs where the
	// first one met NOGROUP still acknowledged nine deliveries, and reporting
	// all ten as failed would make the core retry nine settlements that
	// already happened.
	//
	// The one case where Exec's error belongs to every command is the one
	// where no command has an error of its own: go-redis sets a transport
	// failure on every command it holds and leaves a server's error reply on
	// the command that got it, so "Exec failed and nothing carries an error"
	// means the pipeline never ran -- which is [streams.Commander.DoMulti]'s
	// "no command's outcome is known".
	_, execErr := pipe.Exec(ctx)
	out := make([]streams.Result, len(cmds))
	anyErr := false
	for i, b := range built {
		v, err := convert(b.Result())
		if err != nil {
			anyErr = true
		}
		out[i] = streams.Result{Value: v, Err: err}
	}
	if execErr != nil && !anyErr && !errors.Is(execErr, redis.Nil) {
		return nil, execErr
	}
	return out, nil
}

// Close releases the client when this commander owns it.
func (c *commander) Close() error {
	if !c.own {
		return nil
	}
	return c.client.Close()
}

// Ping is the liveness check one round trip that consumes no message
// and creates no topology.
func (c *commander) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// ServerVersion reads the version out of INFO server.
//
// The capability table branches on it -- WAITAOF needs 7.0, XAUTOCLAIM 6.2 --
// so a driver that could not ask would either claim what it may not have or
// claim nothing. The parsing is [streams.ParseServerInfo], shared with the
// rueidis driver, because telling a Valkey from the Redis version it reports
// for compatibility is a decision and not a detail.
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

// convert turns a go-redis reply into a [streams.Value].
//
// go-redis decodes a reply into `any` -- int64, string, bool, float64,
// *big.Int, []any, map[any]any -- so this is a type switch rather than a
// walk over a library-specific message type, and the two things it has to get
// right are the two the whole command layer depends on.
//
// A NULL reply arrives as an ERROR ([redis.Nil]) rather than as a value, which
// is go-redis's convention and not Redis's: an XREADGROUP that timed out and
// one that failed are different answers, and a layer that passed Nil through
// as a failure would turn every idle fetch into an error. It becomes
// [streams.Nil].
//
// A RESP3 MAP becomes a FLAT array of alternating key and value, which is
// RESP2's shape and the one that preserves the order an entry's fields were
// written in. Only XINFO's reply reaches this driver as a map and it is read
// by name; nothing whose order matters is ever a map, because an entry's
// fields are an array in every RESP version.
func convert(v any, err error) (streams.Value, error) {
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return streams.Nil(), nil
		}
		return streams.Nil(), err
	}
	return convertValue(v)
}

// convertValue is convert's recursive half.
func convertValue(v any) (streams.Value, error) {
	switch t := v.(type) {
	case nil:
		return streams.Nil(), nil
	case int64:
		return streams.Int(t), nil
	case bool:
		return streams.Bool(t), nil
	case string:
		return streams.Str(t), nil
	case []byte:
		return streams.Str(string(t)), nil
	case float64:
		// A float reply is rendered as its string form: nothing this driver
		// sends returns one, and reading it as an integer would round.
		return streams.Str(strconv.FormatFloat(t, 'f', -1, 64)), nil
	case *big.Int:
		// RESP3's big number, likewise as text: it does not fit an int64 by
		// definition, so converting would be lossy.
		if t == nil {
			return streams.Nil(), nil
		}
		return streams.Str(t.String()), nil
	case error:
		// A nested error reply. go-redis puts one in the slice as a value
		// where rueidis fails the whole reply, and the whole reply is the
		// answer that matches: a reply with an error inside it is not data
		// this driver can read, and the two clients must not disagree about
		// which it is.
		return streams.Nil(), t
	case []any:
		out := make([]streams.Value, 0, len(t))
		for _, item := range t {
			child, cerr := convertValue(item)
			if cerr != nil {
				return streams.Nil(), cerr
			}
			out = append(out, child)
		}
		return streams.ArrOf(out), nil
	case map[any]any:
		// Flattened in the key order the map iteration gives, which is not
		// stable -- and that is acceptable for the ONE place a map reply
		// reaches this driver (XINFO's field/value pairs, read by name).
		out := make([]streams.Value, 0, 2*len(t))
		for k, val := range t {
			key, kerr := convertValue(k)
			if kerr != nil {
				return streams.Nil(), kerr
			}
			child, cerr := convertValue(val)
			if cerr != nil {
				return streams.Nil(), cerr
			}
			out = append(out, key, child)
		}
		return streams.ArrOf(out), nil
	}
	return streams.Nil(), errUnknownReply
}

// The commander's own refusals.
var (
	errBadCommand = errcode.New(errcode.InvalidArgument,
		"iqmq/redis/goredis: the command is empty or its key span is outside its tokens")
	errKeyPositionTooFar = errcode.New(errcode.InvalidArgument,
		"iqmq/redis/goredis: the command's first key is beyond the position go-redis can be told about, so a cluster would route it at random")
	errUnknownReply = errcode.New(errcode.CorruptMessage,
		"iqmq/redis/goredis: the client returned a reply of a Go type this driver does not recognise")
	errNilClient = errcode.New(errcode.InvalidConfig,
		"iqmq/redis/goredis: the client is nil")
	errBlockBeyondReadTimeout = errcode.New(errcode.InvalidConfig,
		"iqmq/redis/goredis: Config.Block is not shorter than the client's ReadTimeout, so every fetch that waits that long fails with an i/o timeout")
)
