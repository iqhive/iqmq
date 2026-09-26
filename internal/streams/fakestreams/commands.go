package fakestreams

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/iqhive/iqmq/internal/streams"
)

// xadd implements XADD [NOMKSTREAM] [MAXLEN ~ n] <id|*> field value ...
func (s *Server) xadd(args []string) (streams.Value, error) {
	if len(args) < 3 {
		return streams.Nil(), errBadCommand
	}
	key := args[0]
	i := 1
	nomkstream := false
	var maxLen int64
	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "NOMKSTREAM":
			nomkstream = true
			i++
		case "MAXLEN":
			i++
			if i < len(args) && (args[i] == "~" || args[i] == "=") {
				i++
			}
			if i >= len(args) {
				return streams.Nil(), errBadCommand
			}
			n, err := strconv.ParseInt(args[i], 10, 64)
			if err != nil {
				return streams.Nil(), errBadCommand
			}
			maxLen = n
			i++
		default:
			goto id
		}
	}
id:
	if i >= len(args) {
		return streams.Nil(), errBadCommand
	}
	idArg := args[i]
	fields := args[i+1:]
	if len(fields) == 0 || len(fields)%2 != 0 {
		return streams.Nil(), errBadCommand
	}
	st := s.streamFor(key, !nomkstream)
	if st == nil {
		return streams.Nil(), nil
	}
	var id streams.ID
	if idArg == streams.IDAuto {
		id = s.nextID(st)
	} else {
		parsed, err := streams.ParseID(idArg)
		if err != nil {
			return streams.Nil(), errBadID
		}
		if !st.last.Before(parsed) {
			return streams.Nil(), errBadID
		}
		id = parsed
	}
	st.entries = append(st.entries, entry{id: id, fields: slices.Clone(fields)})
	st.last = id
	if maxLen > 0 && int64(len(st.entries)) > maxLen {
		st.entries = slices.Delete(st.entries, 0, len(st.entries)-int(maxLen))
	}
	defer s.wakeLater()
	return streams.Str(id.String()), nil
}

// wakeLater releases blocked readers after the mutex is dropped. Waking under
// the mutex would deadlock: wake takes it.
func (s *Server) wakeLater() { go s.wake() }

// xrange implements XRANGE key start end [COUNT n].
func (s *Server) xrange(args []string) (streams.Value, error) {
	if len(args) < 3 {
		return streams.Nil(), errBadCommand
	}
	st := s.streams[args[0]]
	if st == nil {
		return streams.ArrOf(nil), nil
	}
	lo, hi, err := parseRange(args[1], args[2])
	if err != nil {
		return streams.Nil(), err
	}
	count := 0
	if len(args) >= 5 && strings.EqualFold(args[3], "COUNT") {
		if count, err = strconv.Atoi(args[4]); err != nil {
			return streams.Nil(), errBadCommand
		}
	}
	var out []streams.Value
	for _, e := range st.entries {
		if e.id.Before(lo) || hi.Before(e.id) {
			continue
		}
		out = append(out, entryValue(e))
		if count > 0 && len(out) >= count {
			break
		}
	}
	return streams.ArrOf(out), nil
}

// parseRange reads a range's bounds, honouring "-" and "+".
func parseRange(start, end string) (lo, hi streams.ID, err error) {
	lo = streams.ID{}
	hi = streams.ID{Ms: ^uint64(0), Seq: ^uint64(0)}
	if start != streams.IDMin {
		if lo, err = streams.ParseID(start); err != nil {
			return lo, hi, errBadID
		}
	}
	if end != streams.IDMax {
		if hi, err = streams.ParseID(end); err != nil {
			return lo, hi, errBadID
		}
	}
	return lo, hi, nil
}

// entryValue renders one entry as [id, [f, v, ...]].
func entryValue(e entry) streams.Value {
	fields := make([]streams.Value, 0, len(e.fields))
	for _, f := range e.fields {
		fields = append(fields, streams.Str(f))
	}
	return streams.Arr(streams.Str(e.id.String()), streams.ArrOf(fields))
}

// xdel implements XDEL key id ...
func (s *Server) xdel(args []string) (streams.Value, error) {
	if len(args) < 2 {
		return streams.Nil(), errBadCommand
	}
	st := s.streams[args[0]]
	if st == nil {
		return streams.Int(0), nil
	}
	var n int64
	for _, idArg := range args[1:] {
		id, err := streams.ParseID(idArg)
		if err != nil {
			return streams.Nil(), errBadID
		}
		if at := st.entryAt(id); at >= 0 {
			st.entries = slices.Delete(st.entries, at, at+1)
			if st.maxDel.Before(id) {
				st.maxDel = id
			}
			n++
		}
	}
	return streams.Int(n), nil
}

// xtrim implements XTRIM key MAXLEN [~|=] n.
func (s *Server) xtrim(args []string) (streams.Value, error) {
	if len(args) < 3 {
		return streams.Nil(), errBadCommand
	}
	if !strings.EqualFold(args[1], "MAXLEN") {
		return streams.Nil(), errBadCommand
	}
	i := 2
	if args[i] == "~" || args[i] == "=" {
		i++
	}
	if i >= len(args) {
		return streams.Nil(), errBadCommand
	}
	maxLen, err := strconv.ParseInt(args[i], 10, 64)
	if err != nil || maxLen < 0 {
		return streams.Nil(), errBadCommand
	}
	st := s.streams[args[0]]
	if st == nil {
		return streams.Int(0), nil
	}
	trimmed := int64(0)
	if int64(len(st.entries)) > maxLen {
		trimmed = int64(len(st.entries)) - maxLen
		st.entries = slices.Delete(st.entries, 0, int(trimmed))
	}
	return streams.Int(trimmed), nil
}

// scan implements SCAN cursor [MATCH pattern] [COUNT n] [TYPE stream].
//
// The cursor is the index into a sorted key list rather than a real
// rehashing cursor, which is enough for what the driver does with it — walk
// every matching key once — and is deliberately NOT a promise the real SCAN
// makes: a real cursor may return a key twice while the table rehashes. The
// driver's ListTopics reports each topic once by construction (it matches
// shard 0's key alone), so the difference does not hide a defect.
func (s *Server) scan(args []string) (streams.Value, error) {
	if len(args) < 1 {
		return streams.Nil(), errBadCommand
	}
	from, err := strconv.Atoi(args[0])
	if err != nil || from < 0 {
		return streams.Nil(), errBadCommand
	}
	pattern, count := "*", 10
	wantStream := false
	for i := 1; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "MATCH":
			if i+1 >= len(args) {
				return streams.Nil(), errBadCommand
			}
			pattern = args[i+1]
			i++
		case "COUNT":
			if i+1 >= len(args) {
				return streams.Nil(), errBadCommand
			}
			if count, err = strconv.Atoi(args[i+1]); err != nil || count <= 0 {
				return streams.Nil(), errBadCommand
			}
			i++
		case "TYPE":
			if i+1 >= len(args) {
				return streams.Nil(), errBadCommand
			}
			wantStream = strings.EqualFold(args[i+1], "stream")
			i++
		default:
			return streams.Nil(), errBadCommand
		}
	}
	all := make([]string, 0, len(s.streams)+len(s.zsets))
	for key := range s.streams {
		all = append(all, key)
	}
	if !wantStream {
		for key := range s.zsets {
			all = append(all, key)
		}
	}
	slices.Sort(all)
	var out []streams.Value
	i := from
	for ; i < len(all) && len(out) < count; i++ {
		if glob(pattern, all[i]) {
			out = append(out, streams.Str(all[i]))
		}
	}
	next := "0"
	if i < len(all) {
		next = strconv.Itoa(i)
	}
	return streams.Arr(streams.Str(next), streams.ArrOf(out)), nil
}

// glob matches Redis's key pattern, reduced to the one operator the driver
// uses: '*' for any run of bytes. A pattern with '?' or '[' is refused rather
// than mis-matched, so a driver that started using one fails here.
func glob(pattern, s string) bool {
	if strings.ContainsAny(pattern, "?[") {
		return false
	}
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == s
	}
	if !strings.HasPrefix(s, parts[0]) {
		return false
	}
	rest := s[len(parts[0]):]
	for _, part := range parts[1 : len(parts)-1] {
		i := strings.Index(rest, part)
		if i < 0 {
			return false
		}
		rest = rest[i+len(part):]
	}
	return strings.HasSuffix(rest, parts[len(parts)-1])
}

// del implements DEL over one or more keys.
func (s *Server) del(args []string) (streams.Value, error) {
	if len(args) < 1 {
		return streams.Nil(), errBadCommand
	}
	var n int64
	for _, key := range args {
		if _, ok := s.streams[key]; ok {
			delete(s.streams, key)
			n++
			continue
		}
		if _, ok := s.zsets[key]; ok {
			delete(s.zsets, key)
			n++
		}
	}
	return streams.Int(n), nil
}

// xgroup implements XGROUP CREATE/SETID/DELCONSUMER/DESTROY.
func (s *Server) xgroup(args []string) (streams.Value, error) {
	if len(args) < 3 {
		return streams.Nil(), errBadCommand
	}
	sub := strings.ToUpper(args[0])
	key, name := args[1], args[2]
	switch sub {
	case "CREATE":
		if len(args) < 4 {
			return streams.Nil(), errBadCommand
		}
		mkstream := len(args) >= 5 && strings.EqualFold(args[4], "MKSTREAM")
		st := s.streamFor(key, mkstream)
		if st == nil {
			return streams.Nil(), ErrNoGroup
		}
		if _, exists := st.groups[name]; exists {
			return streams.Nil(), ErrBusyGroup
		}
		start, err := groupStart(st, args[3])
		if err != nil {
			return streams.Nil(), err
		}
		st.groups[name] = &group{
			last:      start,
			pending:   map[streams.ID]*pel{},
			consumers: map[string]time.Time{},
		}
		return streams.Str("OK"), nil
	case "SETID":
		if len(args) < 4 {
			return streams.Nil(), errBadCommand
		}
		st, g, err := s.groupFor(key, name)
		if err != nil {
			return streams.Nil(), err
		}
		start, err := groupStart(st, args[3])
		if err != nil {
			return streams.Nil(), err
		}
		g.last = start
		return streams.Str("OK"), nil
	case "DELCONSUMER":
		if len(args) < 4 {
			return streams.Nil(), errBadCommand
		}
		_, g, err := s.groupFor(key, name)
		if err != nil {
			return streams.Nil(), err
		}
		consumer := args[3]
		// Redis deletes the consumer's pending entries with it, which is
		// the behaviour a driver must not trip over: the fake does the same
		// so that a driver which called it carelessly fails here.
		removed := int64(0)
		for id, p := range g.pending {
			if p.consumer == consumer {
				delete(g.pending, id)
				removed++
			}
		}
		delete(g.consumers, consumer)
		return streams.Int(removed), nil
	case "DESTROY":
		st := s.streams[key]
		if st == nil {
			return streams.Int(0), nil
		}
		if _, ok := st.groups[name]; !ok {
			return streams.Int(0), nil
		}
		delete(st.groups, name)
		return streams.Int(1), nil
	}
	return streams.Nil(), &ErrUnknownCommand{Verb: "XGROUP " + sub}
}

// groupStart reads XGROUP's start argument.
func groupStart(st *stream, arg string) (streams.ID, error) {
	if arg == streams.IDLast {
		return st.last, nil
	}
	id, err := streams.ParseID(arg)
	if err != nil {
		return streams.ID{}, errBadID
	}
	return id, nil
}

// groupFor returns a stream and one of its groups.
func (s *Server) groupFor(key, name string) (*stream, *group, error) {
	st := s.streams[key]
	if st == nil {
		return nil, nil, ErrNoGroup
	}
	g := st.groups[name]
	if g == nil {
		return nil, nil, ErrNoGroup
	}
	return st, g, nil
}

// xreadgroup implements XREADGROUP GROUP g c [COUNT n] [BLOCK ms] [NOACK]
// STREAMS key... id...
//
// The block is the one place this fake waits. It waits on the notification
// channel rather than polling, so a test that publishes from another
// goroutine is woken rather than timed.
func (s *Server) xreadgroup(ctx context.Context, args []string) (streams.Value, error) {
	req, err := parseReadGroup(args)
	if err != nil {
		return streams.Nil(), err
	}
	deadline := time.Time{}
	if req.block > 0 {
		deadline = time.Now().Add(req.block)
	}
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return streams.Nil(), ErrClosed
		}
		if err, ok := s.faults["XREADGROUP"]; ok {
			delete(s.faults, "XREADGROUP")
			s.mu.Unlock()
			return streams.Nil(), err
		}
		v, found, rerr := s.readGroupLocked(req)
		wait := s.notify
		s.mu.Unlock()
		if rerr != nil {
			return streams.Nil(), rerr
		}
		if found || req.block == 0 {
			if !found {
				return streams.Nil(), nil
			}
			return v, nil
		}
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-ctx.Done():
			timer.Stop()
			return streams.Nil(), ctx.Err()
		case <-wait:
			timer.Stop()
		case <-timer.C:
			return streams.Nil(), nil
		}
		if !time.Now().Before(deadline) {
			return streams.Nil(), nil
		}
	}
}

// readGroup is XREADGROUP's parsed arguments.
type readGroup struct {
	group    string
	consumer string
	keys     []string
	ids      []string
	count    int
	block    time.Duration
	noack    bool
}

func parseReadGroup(args []string) (readGroup, error) {
	var r readGroup
	if len(args) < 5 || !strings.EqualFold(args[0], "GROUP") {
		return r, errBadCommand
	}
	r.group, r.consumer = args[1], args[2]
	i := 3
	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "COUNT":
			if i+1 >= len(args) {
				return r, errBadCommand
			}
			n, err := strconv.Atoi(args[i+1])
			if err != nil || n <= 0 {
				return r, errBadCommand
			}
			r.count = n
			i += 2
		case "BLOCK":
			if i+1 >= len(args) {
				return r, errBadCommand
			}
			d, err := parseMillis(args[i+1])
			if err != nil {
				return r, err
			}
			r.block = d
			i += 2
		case "NOACK":
			r.noack = true
			i++
		case "STREAMS":
			i++
			rest := args[i:]
			if len(rest) == 0 || len(rest)%2 != 0 {
				return r, errBadCommand
			}
			half := len(rest) / 2
			r.keys, r.ids = rest[:half], rest[half:]
			if r.count == 0 {
				r.count = 10
			}
			return r, nil
		default:
			return r, errBadCommand
		}
	}
	return r, errBadCommand
}

// readGroupLocked reads one round.
func (s *Server) readGroupLocked(r readGroup) (streams.Value, bool, error) {
	var out []streams.Value
	now := s.nowLocked()
	for i, key := range r.keys {
		st, g, err := s.groupFor(key, r.group)
		if err != nil {
			return streams.Nil(), false, err
		}
		g.consumers[r.consumer] = now
		var items []streams.Value
		if r.ids[i] == streams.IDNew {
			for _, e := range st.entries {
				if !g.last.Before(e.id) {
					continue
				}
				g.last = e.id
				if !r.noack {
					g.pending[e.id] = &pel{consumer: r.consumer, delivered: now, count: 1}
				}
				items = append(items, entryValue(e))
				if len(items) >= r.count {
					break
				}
			}
		} else {
			// A history read: this consumer's own pending entries from the
			// identifier given. It does NOT touch the delivery counter,
			// which is Redis's behaviour and the reason a driver cannot use
			// it as a redelivery.
			from, perr := streams.ParseID(r.ids[i])
			if perr != nil {
				return streams.Nil(), false, errBadID
			}
			ids := sortedPending(g)
			for _, id := range ids {
				if id.Before(from) || g.pending[id].consumer != r.consumer {
					continue
				}
				at := st.entryAt(id)
				if at < 0 {
					items = append(items, streams.Arr(streams.Str(id.String()), streams.Nil()))
				} else {
					items = append(items, entryValue(st.entries[at]))
				}
				if len(items) >= r.count {
					break
				}
			}
			// A history read always reports a result, even an empty one,
			// which is how a client tells "nothing pending" from "blocked".
			out = append(out, streams.Arr(streams.Str(key), streams.ArrOf(items)))
			continue
		}
		if len(items) > 0 {
			out = append(out, streams.Arr(streams.Str(key), streams.ArrOf(items)))
		}
	}
	if len(out) == 0 {
		return streams.Nil(), false, nil
	}
	return streams.ArrOf(out), true, nil
}

// sortedPending returns a group's pending identifiers in order.
func sortedPending(g *group) []streams.ID {
	ids := make([]streams.ID, 0, len(g.pending))
	for id := range g.pending {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b streams.ID) int { return a.Compare(b) })
	return ids
}

// xack implements XACK key group id ...
//
// It removes the entry WHOEVER holds it, which is Redis's behaviour and the
// whole reason streams.Reclaimer and the guarded acknowledgement exist. A
// fake that checked the owner here would make a driver that acknowledges
// another consumer's delivery pass its tests.
func (s *Server) xack(args []string) (streams.Value, error) {
	if len(args) < 3 {
		return streams.Nil(), errBadCommand
	}
	_, g, err := s.groupFor(args[0], args[1])
	if err != nil {
		return streams.Int(0), nil
	}
	var n int64
	for _, idArg := range args[2:] {
		id, perr := streams.ParseID(idArg)
		if perr != nil {
			return streams.Nil(), errBadID
		}
		if _, ok := g.pending[id]; ok {
			delete(g.pending, id)
			n++
		}
	}
	return streams.Int(n), nil
}

// xautoclaim implements XAUTOCLAIM key group consumer min-idle start
// [COUNT n] [JUSTID].
func (s *Server) xautoclaim(args []string) (streams.Value, error) {
	if len(args) < 5 {
		return streams.Nil(), errBadCommand
	}
	key, groupName, consumer := args[0], args[1], args[2]
	minIdle, err := parseMillis(args[3])
	if err != nil {
		return streams.Nil(), err
	}
	start := streams.ID{}
	if args[4] != streams.IDMin {
		if start, err = streams.ParseID(args[4]); err != nil {
			return streams.Nil(), errBadID
		}
	}
	count, justID := 100, false
	for i := 5; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "COUNT":
			if i+1 >= len(args) {
				return streams.Nil(), errBadCommand
			}
			if count, err = strconv.Atoi(args[i+1]); err != nil || count <= 0 {
				return streams.Nil(), errBadCommand
			}
			i++
		case "JUSTID":
			justID = true
		default:
			return streams.Nil(), errBadCommand
		}
	}
	st, g, err := s.groupFor(key, groupName)
	if err != nil {
		return streams.Nil(), err
	}
	now := s.nowLocked()
	g.consumers[consumer] = now
	var (
		claimed []streams.Value
		deleted []streams.Value
		cursor  = streams.ID{}
		scanned int
	)
	for _, id := range sortedPending(g) {
		if id.Before(start) {
			continue
		}
		if scanned >= count {
			cursor = id
			break
		}
		scanned++
		p := g.pending[id]
		if now.Sub(p.delivered) < minIdle {
			continue
		}
		at := st.entryAt(id)
		if at < 0 {
			// Redis 7.0 drops a pending entry whose stream entry is gone
			// and reports it in the third element.
			delete(g.pending, id)
			deleted = append(deleted, streams.Str(id.String()))
			continue
		}
		p.consumer = consumer
		p.delivered = now
		if !justID {
			// JUSTID does not increment the delivery counter, which is what
			// makes it usable as a lease extension.
			p.count++
			claimed = append(claimed, entryValue(st.entries[at]))
		} else {
			claimed = append(claimed, streams.Str(id.String()))
		}
	}
	return streams.Arr(
		streams.Str(cursor.String()),
		streams.ArrOf(claimed),
		streams.ArrOf(deleted),
	), nil
}

// xclaim implements XCLAIM key group consumer min-idle id ... [JUSTID].
func (s *Server) xclaim(args []string) (streams.Value, error) {
	if len(args) < 5 {
		return streams.Nil(), errBadCommand
	}
	key, groupName, consumer := args[0], args[1], args[2]
	minIdle, err := parseMillis(args[3])
	if err != nil {
		return streams.Nil(), err
	}
	justID := strings.EqualFold(args[len(args)-1], "JUSTID")
	ids := args[4:]
	if justID {
		ids = ids[:len(ids)-1]
	}
	st, g, err := s.groupFor(key, groupName)
	if err != nil {
		return streams.Nil(), err
	}
	now := s.nowLocked()
	g.consumers[consumer] = now
	var out []streams.Value
	for _, idArg := range ids {
		id, perr := streams.ParseID(idArg)
		if perr != nil {
			return streams.Nil(), errBadID
		}
		p, ok := g.pending[id]
		if !ok || now.Sub(p.delivered) < minIdle {
			continue
		}
		at := st.entryAt(id)
		if at < 0 {
			delete(g.pending, id)
			continue
		}
		p.consumer = consumer
		p.delivered = now
		if justID {
			out = append(out, streams.Str(id.String()))
		} else {
			p.count++
			out = append(out, entryValue(st.entries[at]))
		}
	}
	return streams.ArrOf(out), nil
}

// xpending implements the extended XPENDING key group [IDLE ms] start end
// count [consumer].
func (s *Server) xpending(args []string) (streams.Value, error) {
	if len(args) < 5 {
		return streams.Nil(), errBadCommand
	}
	key, groupName := args[0], args[1]
	i := 2
	var idle time.Duration
	if strings.EqualFold(args[i], "IDLE") {
		if i+1 >= len(args) {
			return streams.Nil(), errBadCommand
		}
		d, err := parseMillis(args[i+1])
		if err != nil {
			return streams.Nil(), err
		}
		idle = d
		i += 2
	}
	if i+2 >= len(args) {
		return streams.Nil(), errBadCommand
	}
	lo, hi, err := parseRange(args[i], args[i+1])
	if err != nil {
		return streams.Nil(), err
	}
	count, cerr := strconv.Atoi(args[i+2])
	if cerr != nil || count <= 0 {
		return streams.Nil(), errBadCommand
	}
	filter := ""
	if i+3 < len(args) {
		filter = args[i+3]
	}
	_, g, err := s.groupFor(key, groupName)
	if err != nil {
		return streams.Nil(), err
	}
	now := s.nowLocked()
	var out []streams.Value
	for _, id := range sortedPending(g) {
		if id.Before(lo) || hi.Before(id) {
			continue
		}
		p := g.pending[id]
		if filter != "" && p.consumer != filter {
			continue
		}
		since := now.Sub(p.delivered)
		if since < idle {
			continue
		}
		out = append(out, streams.Arr(
			streams.Str(id.String()),
			streams.Str(p.consumer),
			streams.Int(since.Milliseconds()),
			streams.Int(int64(p.count)),
		))
		if len(out) >= count {
			break
		}
	}
	return streams.ArrOf(out), nil
}

// xinfo implements XINFO GROUPS and XINFO CONSUMERS, enough for the admin
// surface's subscription statistics.
func (s *Server) xinfo(args []string) (streams.Value, error) {
	if len(args) < 2 {
		return streams.Nil(), errBadCommand
	}
	switch strings.ToUpper(args[0]) {
	case "GROUPS":
		st := s.streams[args[1]]
		if st == nil {
			return streams.Nil(), ErrNoGroup
		}
		names := make([]string, 0, len(st.groups))
		for name := range st.groups {
			names = append(names, name)
		}
		slices.Sort(names)
		out := make([]streams.Value, 0, len(names))
		for _, name := range names {
			g := st.groups[name]
			out = append(out, streams.Arr(
				streams.Str("name"), streams.Str(name),
				streams.Str("consumers"), streams.Int(int64(len(g.consumers))),
				streams.Str("pending"), streams.Int(int64(len(g.pending))),
				streams.Str("last-delivered-id"), streams.Str(g.last.String()),
				streams.Str("entries-read"), streams.Int(0),
				streams.Str("lag"), s.lagValue(st, g),
			))
		}
		return streams.ArrOf(out), nil
	case "CONSUMERS":
		if len(args) < 3 {
			return streams.Nil(), errBadCommand
		}
		_, g, err := s.groupFor(args[1], args[2])
		if err != nil {
			return streams.Nil(), err
		}
		names := make([]string, 0, len(g.consumers))
		for name := range g.consumers {
			names = append(names, name)
		}
		slices.Sort(names)
		now := s.nowLocked()
		out := make([]streams.Value, 0, len(names))
		for _, name := range names {
			pending := 0
			for _, p := range g.pending {
				if p.consumer == name {
					pending++
				}
			}
			out = append(out, streams.Arr(
				streams.Str("name"), streams.Str(name),
				streams.Str("pending"), streams.Int(int64(pending)),
				streams.Str("idle"), streams.Int(now.Sub(g.consumers[name]).Milliseconds()),
			))
		}
		return streams.ArrOf(out), nil
	}
	return streams.Nil(), &ErrUnknownCommand{Verb: "XINFO " + strings.ToUpper(args[0])}
}

// lagOf counts the entries a group has not delivered.
// lagValue is the `lag` field of one XINFO GROUPS row.
//
// Redis reports it as NIL once an XDEL or an XTRIM has made the count
// unreliable, and a driver has to tell that apart from a lag of zero --
// again, where an unknown is never an overloaded zero. This server implements
// neither command, so [Server.ReportUnknownLag] is how a test reaches the
// branch; the shape on the wire is the same one Redis sends.
func (s *Server) lagValue(st *stream, g *group) streams.Value {
	if s.unknownLag {
		return streams.Nil()
	}
	return streams.Int(int64(lagOf(st, g)))
}

func lagOf(st *stream, g *group) int {
	n := 0
	for _, e := range st.entries {
		if g.last.Before(e.id) {
			n++
		}
	}
	return n
}

// zadd implements ZADD key score member.
func (s *Server) zadd(args []string) (streams.Value, error) {
	if len(args) != 3 {
		return streams.Nil(), errBadCommand
	}
	score, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return streams.Nil(), errBadCommand
	}
	z := s.zsets[args[0]]
	if z == nil {
		z = &zset{}
		s.zsets[args[0]] = z
	}
	for i := range z.members {
		if z.members[i].member == args[2] {
			z.members[i].score = score
			return streams.Int(0), nil
		}
	}
	z.members = append(z.members, zmember{score: score, member: args[2]})
	return streams.Int(1), nil
}

// zrem implements ZREM key member ...
func (s *Server) zrem(args []string) (streams.Value, error) {
	if len(args) < 2 {
		return streams.Nil(), errBadCommand
	}
	z := s.zsets[args[0]]
	if z == nil {
		return streams.Int(0), nil
	}
	var n int64
	for _, m := range args[1:] {
		for i := range z.members {
			if z.members[i].member == m {
				z.members = slices.Delete(z.members, i, i+1)
				n++
				break
			}
		}
	}
	return streams.Int(n), nil
}

// zrangebyscore implements ZRANGEBYSCORE key -inf max [LIMIT off count].
func (s *Server) zrangebyscore(args []string) (streams.Value, error) {
	if len(args) < 3 {
		return streams.Nil(), errBadCommand
	}
	maxScore, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return streams.Nil(), errBadCommand
	}
	limit := 0
	if len(args) >= 6 && strings.EqualFold(args[3], "LIMIT") {
		if limit, err = strconv.Atoi(args[5]); err != nil {
			return streams.Nil(), errBadCommand
		}
	}
	z := s.zsets[args[0]]
	if z == nil {
		return streams.ArrOf(nil), nil
	}
	ordered := slices.Clone(z.members)
	slices.SortFunc(ordered, func(a, b zmember) int {
		if a.score != b.score {
			return int(a.score - b.score)
		}
		return strings.Compare(a.member, b.member)
	})
	var out []streams.Value
	for _, m := range ordered {
		if m.score > maxScore {
			break
		}
		out = append(out, streams.Str(m.member))
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return streams.ArrOf(out), nil
}

// waitaof implements WAITAOF numlocal numreplicas timeout.
func (s *Server) waitaof(args []string) (streams.Value, error) {
	if len(args) != 3 {
		return streams.Nil(), errBadCommand
	}
	numLocal, err := strconv.Atoi(args[0])
	if err != nil {
		return streams.Nil(), errBadCommand
	}
	if numLocal > 0 && !s.aof {
		return streams.Nil(), ErrNoAOF
	}
	local := int64(0)
	if s.aof {
		local = 1
	}
	return streams.Arr(streams.Int(local), streams.Int(0)), nil
}

// script implements SCRIPT LOAD.
func (s *Server) script(args []string) (streams.Value, error) {
	if len(args) != 2 || !strings.EqualFold(args[0], "LOAD") {
		return streams.Nil(), errBadCommand
	}
	sha := streams.SHA(args[1])
	if _, known := s.scripts[sha]; !known {
		// Loading a script this fake cannot execute would let a driver
		// believe its new script works. Refusing names the digest.
		return streams.Nil(), ErrNoScript
	}
	return streams.Str(sha), nil
}
