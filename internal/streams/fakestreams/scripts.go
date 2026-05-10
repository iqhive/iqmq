package fakestreams

import (
	"strconv"

	"bitbucket.org/iqhive/iqmq/internal/streams"
)

// eval executes one of the package's scripts by its digest.
//
// This is not a Lua interpreter and does not pretend to be one. What it
// reproduces is the property the scripts are used FOR: each runs to
// completion with nothing interleaved, because eval is called with the
// server's mutex held. The Go below has to be kept in step with the Lua by
// hand, and two things keep that honest — a digest this fake does not know is
// NOSCRIPT rather than a no-op, so a script added without a body here fails
// every test that uses it, and the conformance suite runs the real Lua
// against a real container.
//
// args is EVALSHA's tail: numkeys, the keys, then the script arguments.
func (s *Server) eval(sha string, args []string) (streams.Value, error) {
	if len(args) < 1 {
		return streams.Nil(), errBadCommand
	}
	numKeys, err := strconv.Atoi(args[0])
	if err != nil || numKeys < 0 || 1+numKeys > len(args) {
		return streams.Nil(), errBadCommand
	}
	keys := args[1 : 1+numKeys]
	argv := args[1+numKeys:]
	if !streams.SlotSafe(keys...) {
		// A cluster refuses a script whose keys are on two slots. The fake
		// is not a cluster, but refusing here is what makes a driver that
		// forgot the check fail in a unit test rather than only against a
		// cluster nobody runs in CI.
		return streams.Nil(), ErrCrossSlot
	}
	switch sha {
	case streams.SHAOf(streams.ScriptAckGuard):
		return s.evalAckGuard(keys, argv)
	case streams.SHAOf(streams.ScriptExtendGuard):
		return s.evalExtendGuard(keys, argv)
	case streams.SHAOf(streams.ScriptDeadLetter):
		return s.evalDeadLetter(keys, argv)
	case streams.SHAOf(streams.ScriptDelayPark):
		return s.evalDelayPark(keys, argv)
	case streams.SHAOf(streams.ScriptDelayRelease):
		return s.evalDelayRelease(keys, argv)
	case streams.SHAOf(streams.ScriptReAdd):
		return s.evalReAdd(keys, argv)
	}
	return streams.Nil(), ErrNoScript
}

// evalAckGuard is ScriptAckGuard: acknowledge only what this consumer holds.
func (s *Server) evalAckGuard(keys, argv []string) (streams.Value, error) {
	if len(keys) != 1 || len(argv) < 3 {
		return streams.Nil(), errBadCommand
	}
	_, g, err := s.groupFor(keys[0], argv[0])
	if err != nil {
		// A group that does not exist holds nothing, so every identifier is
		// lost. Reporting the error instead would make a settlement look
		// retryable when there is nothing to retry against.
		lost := make([]streams.Value, 0, len(argv)-2)
		for _, id := range argv[2:] {
			lost = append(lost, streams.Str(id))
		}
		return streams.Arr(streams.Int(0), streams.ArrOf(lost)), nil
	}
	me := argv[1]
	var acked int64
	var lost []streams.Value
	for _, idArg := range argv[2:] {
		id, perr := streams.ParseID(idArg)
		if perr != nil {
			return streams.Nil(), errBadID
		}
		p, ok := g.pending[id]
		if !ok || p.consumer != me {
			lost = append(lost, streams.Str(idArg))
			continue
		}
		delete(g.pending, id)
		acked++
	}
	return streams.Arr(streams.Int(acked), streams.ArrOf(lost)), nil
}

// evalExtendGuard is ScriptExtendGuard: extend only what this consumer holds.
func (s *Server) evalExtendGuard(keys, argv []string) (streams.Value, error) {
	if len(keys) != 1 || len(argv) < 3 {
		return streams.Nil(), errBadCommand
	}
	_, g, err := s.groupFor(keys[0], argv[0])
	if err != nil {
		lost := make([]streams.Value, 0, len(argv)-2)
		for _, id := range argv[2:] {
			lost = append(lost, streams.Str(id))
		}
		return streams.Arr(streams.ArrOf(nil), streams.ArrOf(lost)), nil
	}
	me := argv[1]
	now := s.nowLocked()
	var extended, lost []streams.Value
	for _, idArg := range argv[2:] {
		id, perr := streams.ParseID(idArg)
		if perr != nil {
			return streams.Nil(), errBadID
		}
		p, ok := g.pending[id]
		if !ok || p.consumer != me {
			lost = append(lost, streams.Str(idArg))
			continue
		}
		// JUSTID resets the idle clock and leaves the delivery counter
		// alone, which is the whole reason the extension uses it.
		p.delivered = now
		extended = append(extended, streams.Str(idArg))
	}
	return streams.Arr(streams.ArrOf(extended), streams.ArrOf(lost)), nil
}

// evalDeadLetter is ScriptDeadLetter: copy to the dead-letter stream and
// acknowledge, atomically.
func (s *Server) evalDeadLetter(keys, argv []string) (streams.Value, error) {
	if len(keys) != 2 || len(argv) < 4 {
		return streams.Nil(), errBadCommand
	}
	_, g, err := s.groupFor(keys[0], argv[0])
	if err != nil {
		return streams.Nil(), nil
	}
	id, perr := streams.ParseID(argv[2])
	if perr != nil {
		return streams.Nil(), errBadID
	}
	p, ok := g.pending[id]
	if !ok || p.consumer != argv[1] {
		return streams.Nil(), nil
	}
	fields := argv[4:]
	if len(fields) == 0 || len(fields)%2 != 0 {
		return streams.Nil(), errBadCommand
	}
	add := append([]string{keys[1]}, maxLenArgs(argv[3])...)
	add = append(add, streams.IDAuto)
	add = append(add, fields...)
	newID, aerr := s.xadd(add)
	if aerr != nil {
		return streams.Nil(), aerr
	}
	delete(g.pending, id)
	return newID, nil
}

// evalDelayPark is ScriptDelayPark: park in the sorted set and acknowledge.
func (s *Server) evalDelayPark(keys, argv []string) (streams.Value, error) {
	if len(keys) != 2 || len(argv) != 5 {
		return streams.Nil(), errBadCommand
	}
	_, g, err := s.groupFor(keys[0], argv[0])
	if err != nil {
		return streams.Nil(), nil
	}
	id, perr := streams.ParseID(argv[2])
	if perr != nil {
		return streams.Nil(), errBadID
	}
	p, ok := g.pending[id]
	if !ok || p.consumer != argv[1] {
		return streams.Nil(), nil
	}
	if _, zerr := s.zadd([]string{keys[1], argv[3], argv[4]}); zerr != nil {
		return streams.Nil(), zerr
	}
	delete(g.pending, id)
	return streams.Int(1), nil
}

// evalDelayRelease is ScriptDelayRelease: remove from the sorted set and add
// to the stream, the removal deciding the winner.
func (s *Server) evalDelayRelease(keys, argv []string) (streams.Value, error) {
	if len(keys) != 2 || len(argv) < 3 {
		return streams.Nil(), errBadCommand
	}
	removed, err := s.zrem([]string{keys[0], argv[0]})
	if err != nil {
		return streams.Nil(), err
	}
	if n, _ := removed.Int64(); n == 0 {
		return streams.Nil(), nil
	}
	fields := argv[2:]
	if len(fields)%2 != 0 {
		return streams.Nil(), errBadCommand
	}
	add := append([]string{keys[1]}, maxLenArgs(argv[1])...)
	add = append(add, streams.IDAuto)
	add = append(add, fields...)
	return s.xadd(add)
}

// evalReAdd is ScriptReAdd: add the entry back and acknowledge the old one.
func (s *Server) evalReAdd(keys, argv []string) (streams.Value, error) {
	if len(keys) != 1 || len(argv) < 4 {
		return streams.Nil(), errBadCommand
	}
	_, g, err := s.groupFor(keys[0], argv[0])
	if err != nil {
		return streams.Nil(), nil
	}
	id, perr := streams.ParseID(argv[2])
	if perr != nil {
		return streams.Nil(), errBadID
	}
	p, ok := g.pending[id]
	if !ok || p.consumer != argv[1] {
		return streams.Nil(), nil
	}
	fields := argv[4:]
	if len(fields) == 0 || len(fields)%2 != 0 {
		return streams.Nil(), errBadCommand
	}
	add := append([]string{keys[0]}, maxLenArgs(argv[3])...)
	add = append(add, streams.IDAuto)
	add = append(add, fields...)
	newID, aerr := s.xadd(add)
	if aerr != nil {
		return streams.Nil(), aerr
	}
	delete(g.pending, id)
	return newID, nil
}

// maxLenArgs renders the MAXLEN clause a script's maxlen argument asks for.
func maxLenArgs(arg string) []string {
	n, err := strconv.ParseInt(arg, 10, 64)
	if err != nil || n <= 0 {
		return nil
	}
	return []string{"MAXLEN", "~", strconv.FormatInt(n, 10)}
}

// Pending returns a group's pending-entries list, for a test that wants to
// assert on it between two commands.
//
// It is the fake's whole reason for existing: the pending-entries list is
// where a Redis Streams driver loses or duplicates a message, and against a
// real server it can only be observed with another round trip that changes
// the idle times the next reclaim reads.
func (s *Server) Pending(key, groupName string) []streams.Pending {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, g, err := s.groupFor(key, groupName)
	if err != nil {
		return nil
	}
	now := s.nowLocked()
	out := make([]streams.Pending, 0, len(g.pending))
	for _, id := range sortedPending(g) {
		p := g.pending[id]
		out = append(out, streams.Pending{
			ID:        id,
			Consumer:  p.consumer,
			Idle:      now.Sub(p.delivered),
			Delivered: p.count,
		})
	}
	return out
}

// Entries returns a stream's entries, for the same reason.
func (s *Server) Entries(key string) []streams.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.streams[key]
	if st == nil {
		return nil
	}
	out := make([]streams.Entry, 0, len(st.entries))
	for _, e := range st.entries {
		out = append(out, streams.Entry{ID: e.id, Shard: -1, Fields: e.fields})
	}
	return out
}

// ZLen returns how many members a sorted set holds.
func (s *Server) ZLen(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if z := s.zsets[key]; z != nil {
		return len(z.members)
	}
	return 0
}

// Deliver hands an entry to a consumer directly, bypassing XREADGROUP.
//
// It exists for the one situation a test cannot otherwise construct: another
// PROCESS holding a pending entry. Everything else goes through the commands.
func (s *Server) Deliver(key, groupName, consumer string, id streams.ID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, g, err := s.groupFor(key, groupName)
	if err != nil {
		return false
	}
	now := s.nowLocked()
	if p, ok := g.pending[id]; ok {
		p.consumer = consumer
		p.delivered = now
		p.count++
		return true
	}
	g.pending[id] = &pel{consumer: consumer, delivered: now, count: 1}
	g.consumers[consumer] = now
	return true
}
