package engine

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
)

// This file is the class-level check of the park/notify family, and it exists
// because four separate incidents in this repository have had one shape: a
// permit or a slot that somebody parks on, returned on a withdrawal path that
// skips the notification its ordinary counterpart makes. dispatch.gate
// lane.reserve (again), Engine.reserveTotal
// and pendingBound.admit (the specification) were each found by a careful review of the
// same code, and the review after the fourth concluded that what this needs
// is a model of the pair rather than a fifth review. Four more instances came
// out of the audit that followed and are fixed beside this file (to).
//
// The mechanism is a source scan rather than a TLA+ specification, and the
// reason is what the check has to be able to fail on. All eight instances are
// defects in the *mapping* from this code to the abstraction, not in the
// abstraction: every one of them is a Go statement that lowers a counter, or
// a Go park condition that reads the wrong half of one. A specification of
// the park/notify protocol would state the invariant those statements break
// and would go on passing with any of them reintroduced, because nothing
// connects a TLC run to `e.msgs.Add(-n)`; tools/modelcheck only holds the
// spec's invariant names to the Go model's doc comment, which is a name
// check. So the model would have been a fifth statement of the rule and not a
// check of it, and this check reads the code.
//
// What it does NOT check: the ORDER of a write and the notification that
// covers it. A function that announces the fall before making it passes here,
// and the notifyGate's own contract is what makes that safe on the paths
// this package has -- a waiter registers before its last look, so an early
// wake makes it look again rather than losing the wake. A check of the order
// wants the model this one replaced.

// gateDecl is one gate of this package: a thing a goroutine parks on, with
// the methods that announce a change to it and the methods that register a
// waiter on it.
type gateDecl struct {
	owner, field string
	// notifiers are the methods that wake the gate's waiters.
	notifiers []string
	// registrars are the methods a waiter calls to register on the gate. A
	// function that calls one is a parker and must be in parkCases.
	registrars []string
	// why says what the gate is for.
	why string
}

func (g gateDecl) name() string { return g.owner + "." + g.field }

// parkGates is every gate in internal/engine.
// TestEveryGateIsDeclared holds the list to the package's own fields, so a
// gate somebody adds is a failure here until it is declared with the counters
// it guards.
var parkGates = []gateDecl{
	{
		owner: "Engine", field: "room", notifiers: []string{"notify"}, registrars: []string{"chanFor"},
		why: "the engine-wide budget a refused publisher waits for (Topic.waitRoom, Topic.waitLogRoom)",
	},
	{
		owner: "Engine", field: "idle", notifiers: []string{"notify"}, registrars: []string{"chanFor"},
		why: "Engine.WaitIdle, whose IdleIncludingDelayed condition is the engine-wide record count alone",
	},
	{
		owner: "Subscription", field: "room", notifiers: []string{"notify"}, registrars: []string{"chanFor"},
		why: "the lane capacity a publisher blocked under OverflowBlock waits for (Topic.waitRoom)",
	},
	{
		owner: "logRing", field: "room", notifiers: []string{"notify"}, registrars: []string{"chanFor"},
		why: "the shared log's capacity (Topic.waitLogRoom)",
	},
	{
		owner: "Topic", field: "room", notifiers: []string{"notify"}, registrars: []string{"chanFor"},
		why: "the capacity of any lane of the topic, which a publisher of a fan-out that admits a record " +
			"wherever it fits waits for (Topic.waitAnyRoom), and a DropSlow publisher on a shared log " +
			"reads through the lanes' gauges (Topic.waitLogRoom)",
	},
	{
		owner: "Subscription", field: "waiters", notifiers: []string{"Notify", "NotifyAll"},
		registrars: []string{"Prepare"},
		why:        "the records and the assignment a parked Consumer.Fetch waits for (internal/wait)",
	},
}

// fieldDecl is one piece of state somebody's park condition reads, together
// with the gates that have to be told when it changes.
type fieldDecl struct {
	owner, field string
	// gates are every gate whose waiters' condition reads this field. A
	// change in it has to be announced on all of them, because any of their
	// waiters may be the one the change satisfies.
	gates []string
	// anyWrite makes every write to the field a change that has to be
	// announced. It is for state that is replaced rather than counted, where
	// there is no direction to read.
	anyWrite bool
	why      string
}

func (f fieldDecl) name() string { return f.owner + "." + f.field }

// guardedFields is the state of this package that somebody parks on.
// TestEveryFallInGuardedStateIsAnnounced holds every write to one of them to
// the announcement, and TestEveryParkIsOnTheWholeCondition holds every park
// to the fields its own admission is refused by.
var guardedFields = []fieldDecl{
	{
		owner: "Engine", field: "msgs", gates: []string{"Engine.room", "Engine.idle"},
		why: "the engine-wide record count: Engine.hasTotalRoom reads it for a refused publisher and " +
			"Engine.isIdle reads it as the whole of the IdleIncludingDelayed condition",
	},
	{
		owner: "Engine", field: "bytes", gates: []string{"Engine.room"},
		why: "the engine-wide byte count, which Engine.hasTotalRoom reads; isIdle does not read it",
	},
	{
		owner: "lane", field: "depth", gates: []string{"Subscription.room", "Topic.room"},
		why: "the lane's record count, which lane.hasRoom reads for a publisher under OverflowBlock " +
			"waiting for this subscription or for any lane of its topic",
	},
	{
		owner: "lane", field: "bytes", gates: []string{"Subscription.room", "Topic.room"},
		why: "the lane's byte count, which lane.hasRoom reads",
	},
	{
		owner: "logRing", field: "pending", gates: []string{"logRing.room"},
		why: "reservations taken but not appended, which logRing.hasRoom counts against the log's capacity",
	},
	{
		owner: "logRing", field: "bytes", gates: []string{"logRing.room"},
		why: "the log's byte count, which logRing.hasRoom reads",
	},
	{
		owner: "Subscription", field: "assign", gates: []string{"Subscription.waiters"}, anyWrite: true,
		why: "the sticky assignment: lane.assigned reads it to decide which lanes a parked fetch may " +
			"claim from, so replacing the table changes that fetch's condition ",
	},
}

// silentDecl is one write to guarded state whose own function does not
// announce it, with what makes that sound.
type silentDecl struct {
	// fn is the writing function ("Owner.method"), field the state it
	// writes.
	fn, field string
	// viaCallers delegates the announcement to the callers: this check then
	// requires that the function has in-package callers and that every one
	// of them announces the change on every gate the field names. It is what
	// a helper called under a mutex its caller releases before announcing
	// needs, and it is checked rather than asserted.
	viaCallers bool
	// why is the reason, for the writes no scan can check.
	why string
}

func (s silentDecl) key() string { return s.fn + " -> " + s.field }

// silentWrites is every write to guarded state that does not announce itself,
// with the reason. An entry that stops being needed is a failure too
// (TestEveryFallInGuardedStateIsAnnounced), so the list cannot rot into a
// list of things somebody once decided not to think about.
var silentWrites = []silentDecl{
	{
		fn: "logRing.trimLocked", field: "logRing.bytes", viaCallers: true,
		why: "it runs under the ring mutex and its caller announces once it has released it",
	},
	{
		fn: "logRing.append", field: "logRing.pending",
		why: "the reservation becomes a slot in the same section that takes it (head++), so " +
			"head-base+pending -- the count logRing.hasRoom compares against the capacity -- does " +
			"not fall at all",
	},
	{
		fn: "Subscription.rebalanceLocked", field: "Subscription.assign", viaCallers: true,
		why: "it runs under the membership mutex and every caller announces the new table through " +
			"Subscription.revoke once it has released it ",
	},
	{
		fn: "Topic.subscribe", field: "Subscription.assign",
		why: "the first table of a subscription that no goroutine can reach yet: Topic.storeSubs " +
			"publishes it afterwards, so there is nobody parked on it to tell",
	},
}

// parkCase is one park in this package: the gates it registers on, the
// functions whose result is its condition, the guarded state that condition
// reads, and the admission whose refusal sends a caller here.
type parkCase struct {
	// fn is the parking function ("Owner.method").
	fn string
	// gates are the gates it registers on. The scan checks this against the
	// source in both directions, so a park that grows or loses a gate fails
	// here.
	gates []string
	// conditions are the functions whose result it parks on, each of which
	// the scan requires it to call.
	conditions []string
	// fields is the guarded state the condition depends on. The scan
	// requires each to be read by one of the conditions, and requires the
	// gates above to include a gate of each: state nobody announces on a
	// gate this park holds is state this park can miss.
	fields []string
	// impure says why the declared conditions are not pure predicates, and
	// waives the check that they read nothing beyond fields. A condition
	// that also changes state -- Consumer.take claims, leases, duplicates
	// and drops -- reaches every capacity counter in the engine through
	// calls it makes rather than through the question it answers, so its
	// read set says nothing about what it depends on. Empty means the
	// conditions are predicates and the check applies.
	impure string
	// retries is the admission the caller retries after this park, or empty
	// when the park is not a retry loop. Everything that admission books has
	// to be in fields: a park that re-checks less than its admission needs
	// returns at once, is refused again and spins, which is what
	// Topic.waitRoom did.
	retries string
	why     string
}

// parkCases is every park in internal/engine.
// TestEveryParkIsOnTheWholeCondition finds the parks in the source and fails
// on one that is not here.
var parkCases = []parkCase{
	{
		fn:         "Topic.waitRoom",
		gates:      []string{"Subscription.room", "Engine.room"},
		conditions: []string{"lane.hasRoom", "Engine.hasTotalRoom"},
		fields:     []string{"lane.depth", "lane.bytes", "Engine.msgs", "Engine.bytes"},
		retries:    "Topic.reserveAll",
		why: "a publisher blocked under OverflowBlock on a per-subscription lane: reserveAll books " +
			"the lane and the engine-wide ledger, so both refuse it and both have to wake it",
	},
	{
		fn:         "Topic.waitAnyRoom",
		gates:      []string{"Topic.room", "Engine.room"},
		conditions: []string{"Topic.admitsSomewhere"},
		fields:     []string{"lane.depth", "lane.bytes", "Engine.msgs", "Engine.bytes"},
		retries:    "Topic.reserveAll",
		why: "the same publisher under a fan-out that admits wherever the record fits: any lane of the " +
			"publication can take it, so it waits on the topic's gate, which every lane announces on. " +
			"Under DropSlow a record a lease gives back unsettled is one a drop may take, which the " +
			"lane's in-flight list says and no counter does, so lane.requeue announces it on the same gate",
	},
	{
		fn:         "Topic.waitLogRoom",
		gates:      []string{"logRing.room", "Engine.room", "Topic.room"},
		conditions: []string{"logRing.hasRoom", "Topic.displacementMakesRoom"},
		fields: []string{
			"logRing.pending", "logRing.bytes", "Engine.msgs", "Engine.bytes", "lane.depth", "lane.bytes",
		},
		retries: "logRing.reserve",
		why: "the same publisher on a shared partition log, whose reservation is the log's slot plus the " +
			"engine's. Under DropSlow it also waits for a displacement that would make room, which reads " +
			"the lanes' gauges, so it parks on the topic's gate as well",
	},
	{
		fn:         "Engine.WaitIdle",
		gates:      []string{"Engine.idle"},
		conditions: []string{"Engine.isIdle"},
		fields:     []string{"Engine.msgs"},
		impure: "isIdle takes a whole Subscription.Inspect snapshot for the lane-walking modes, so it " +
			"reads the lanes' byte gauge as a by-product of counting what is ready and in flight",
		why: "the memory engine's idle wait. It books nothing, so it retries nothing; its condition " +
			"is the engine-wide record count and, in the lane-walking modes, the lanes",
	},
	{
		fn:         "Consumer.Fetch",
		gates:      []string{"Subscription.waiters"},
		conditions: []string{"Consumer.take"},
		fields:     []string{"Subscription.assign"},
		impure: "take is the whole claim path and not a predicate: it leases, duplicates and drops, so " +
			"its closure books and returns capacity in both the lanes and the engine-wide ledger",
		why: "a consumer waiting for a record. What it parks on is the lanes' contents, which are " +
			"not counters, and the assignment that decides which lanes it may read at all",
	},
}

// ---------------------------------------------------------------------------
// The scan
// ---------------------------------------------------------------------------

// writeSite is one write to guarded state.
type writeSite struct {
	field string
	pos   token.Position
	// lowers reports that the write can lower the value. A raise cannot
	// satisfy a condition of the form "is there room" or "is it empty", so
	// only a fall has to be announced -- but a raise is what a park has to
	// be able to see (parkCase.retries).
	lowers bool
	// what is the operation, for the failure message.
	what string
	// reached are the statement lists that lead to this write, innermost
	// first: the write happens only if every one of them was entered, so an
	// announcement made by a direct statement of any of them is an
	// announcement this write cannot avoid. One made inside a conditional of
	// its own is not, which is the distinction the fourth finding turned on
	// -- Subscription.revoke announced the new assignment only when it had
	// requeued a lease.
	reached []*listInfo
}

// listInfo is what one statement list does directly: the gates its own
// statements announce on, and the functions of this package they call. A
// statement nested in a conditional inside the list belongs to that
// conditional's list and not to this one.
type listInfo struct {
	notifies map[string]bool
	calls    map[string]bool
}

// scan is the package's source, resolved far enough to attribute a write to a
// field of a type and a call to a method of a type.
type scan struct {
	fset *token.FileSet
	// fields maps a type name to its field names and the bare type name of
	// each, which is what resolves a selector chain.
	fields map[string]map[string]string
	// funcs maps "Owner.method" (or ".function") to its declaration.
	funcs map[string]*ast.FuncDecl
	// calls, writes and reads are per function key. calls is every call the
	// function makes, conditional or not, which is what an admission books
	// and a condition reads through.
	calls  map[string]map[string]bool
	writes map[string][]writeSite
	reads  map[string]map[string]bool
	// body is each function's top-level statement list, and lists maps every
	// statement list of the package to what it announces directly. Together
	// they answer "is this announcement reached whenever the write is".
	body  map[string]*listInfo
	lists map[ast.Node]*listInfo
	// unconditionalCache memoises unconditional().
	unconditionalCache map[string]map[string]bool
	// registers maps a function to the gates it registers a waiter on, and
	// parks to the position of the registration that makes it a parker.
	registers map[string]map[string]bool
	parks     map[string]token.Position
	// unattributed collects write-shaped operations on a field name a
	// guarded field uses, whose owning type this scan could not resolve. A
	// check that cannot see a write cannot fail on it, so these are
	// failures.
	unattributed []string
}

// loadScan parses every non-test file of this package and resolves it.
func loadScan(t *testing.T) *scan {
	t.Helper()
	s := &scan{
		fset:   token.NewFileSet(),
		fields: map[string]map[string]string{},
		funcs:  map[string]*ast.FuncDecl{},
		calls:  map[string]map[string]bool{},
		writes: map[string][]writeSite{},
		reads:  map[string]map[string]bool{},
		body:   map[string]*listInfo{},
		lists:  map[ast.Node]*listInfo{},

		unconditionalCache: map[string]map[string]bool{},
		registers:          map[string]map[string]bool{},
		parks:              map[string]token.Position{},
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(s.fset, filepath.Clean(name), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		files = append(files, f)
	}
	if len(files) == 0 {
		t.Fatal("the scan parsed no file of this package, so it cannot fail")
	}
	// Pass one: the struct fields, which everything else resolves through.
	for _, f := range files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}
			for _, spec := range gd.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					continue
				}
				m := map[string]string{}
				for _, fld := range st.Fields.List {
					for _, n := range fld.Names {
						m[n.Name] = bareType(fld.Type)
					}
				}
				s.fields[ts.Name.Name] = m
			}
		}
	}
	// Pass two: the functions.
	for _, f := range files {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			s.funcs[declKey(fn)] = fn
		}
	}
	// Pass three: what each function does.
	for key, fn := range s.funcs {
		s.walk(key, fn)
	}
	return s
}

// declKey is the scan's name for a declaration: "Owner.method" for a method
// and ".function" for a package-level function.
func declKey(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return "." + fn.Name.Name
	}
	return bareType(fn.Recv.List[0].Type) + "." + fn.Name.Name
}

// bareType is the type name a type expression names, with the pointers,
// slices, maps and type arguments taken off. A type from another package has
// none, which is what makes the resolution stop at the package boundary.
func bareType(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return bareType(t.X)
	case *ast.ParenExpr:
		return bareType(t.X)
	case *ast.ArrayType:
		return bareType(t.Elt)
	case *ast.MapType:
		return bareType(t.Value)
	case *ast.IndexExpr:
		return bareType(t.X)
	case *ast.IndexListExpr:
		return bareType(t.X)
	}
	return ""
}

// env is the identifier-to-type table of one function body.
type env map[string]string

// envFor builds the table from the receiver, the parameters, the results and
// the definitions in the body. Two passes, because a definition may name an
// identifier a later one resolves through.
func (s *scan) envFor(fn *ast.FuncDecl) env {
	e := env{}
	add := func(fl *ast.FieldList) {
		if fl == nil {
			return
		}
		for _, f := range fl.List {
			for _, n := range f.Names {
				if bt := bareType(f.Type); bt != "" {
					e[n.Name] = bt
				}
			}
		}
	}
	add(fn.Recv)
	add(fn.Type.Params)
	add(fn.Type.Results)
	for range 2 {
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch st := n.(type) {
			case *ast.AssignStmt:
				if st.Tok != token.DEFINE || len(st.Lhs) != len(st.Rhs) {
					return true
				}
				for i, lhs := range st.Lhs {
					id, ok := lhs.(*ast.Ident)
					if !ok {
						continue
					}
					if tn := s.typeOf(e, st.Rhs[i]); tn != "" {
						e[id.Name] = tn
					}
				}
			case *ast.RangeStmt:
				if id, ok := st.Value.(*ast.Ident); ok {
					if tn := s.typeOf(e, st.X); tn != "" {
						e[id.Name] = tn
					}
				}
			}
			return true
		})
	}
	return e
}

// typeOf resolves the bare type name of an expression, or "" when it cannot.
// It follows selector chains through the struct fields, so t.eng.msgs
// resolves as far as the engine, and indexing and address-of do not stop it.
func (s *scan) typeOf(e env, x ast.Expr) string {
	switch t := x.(type) {
	case *ast.Ident:
		return e[t.Name]
	case *ast.SelectorExpr:
		owner := s.typeOf(e, t.X)
		if owner == "" {
			return ""
		}
		return s.fields[owner][t.Sel.Name]
	case *ast.IndexExpr:
		return s.typeOf(e, t.X)
	case *ast.StarExpr:
		return s.typeOf(e, t.X)
	case *ast.ParenExpr:
		return s.typeOf(e, t.X)
	case *ast.UnaryExpr:
		if t.Op == token.AND {
			return s.typeOf(e, t.X)
		}
	case *ast.CompositeLit:
		return bareType(t.Type)
	}
	return ""
}

// selectorField resolves an expression of the form <something>.<field> to
// "Owner.field", or returns "" when it is not one or cannot be attributed.
// name is the field name it saw either way, which is what lets the caller
// report a write it could not attribute.
func (s *scan) selectorField(e env, x ast.Expr) (attributed, name string) {
	sel, ok := x.(*ast.SelectorExpr)
	if !ok {
		return "", ""
	}
	owner := s.typeOf(e, sel.X)
	if owner == "" {
		return "", sel.Sel.Name
	}
	return owner + "." + sel.Sel.Name, sel.Sel.Name
}

// certain reports whether a statement is reached whenever its own list is:
// anything but a conditional, a loop, a branch or a goroutine. The parts of a
// conditional that are certain -- its initialiser and its condition -- are
// deliberately not counted: nothing in this package announces from one, and
// counting them would be the first step towards a control-flow analysis this
// check does not want to be.
func certain(st ast.Stmt) bool {
	switch st.(type) {
	case *ast.ExprStmt, *ast.DeferStmt, *ast.AssignStmt, *ast.DeclStmt,
		*ast.IncDecStmt, *ast.ReturnStmt, *ast.SendStmt:
		return true
	}
	return false
}

// listInfoFor records what a statement list announces and calls directly. A
// function literal is a boundary: what it does happens when somebody calls
// it, which is not "whenever this list is entered".
func (s *scan) listInfoFor(e env, stmts []ast.Stmt) *listInfo {
	li := &listInfo{notifies: map[string]bool{}, calls: map[string]bool{}}
	gateByName := map[string]gateDecl{}
	for _, g := range parkGates {
		gateByName[g.name()] = g
	}
	for _, st := range stmts {
		if !certain(st) {
			continue
		}
		ast.Inspect(st, func(n ast.Node) bool {
			if _, ok := n.(*ast.FuncLit); ok {
				return false
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				if id, ok := call.Fun.(*ast.Ident); ok && s.funcs["."+id.Name] != nil {
					li.calls["."+id.Name] = true
				}
				return true
			}
			if attributed, _ := s.selectorField(e, sel.X); attributed != "" {
				if g, ok := gateByName[attributed]; ok && slices.Contains(g.notifiers, sel.Sel.Name) {
					li.notifies[attributed] = true
				}
			}
			if owner := s.typeOf(e, sel.X); owner != "" {
				if k := owner + "." + sel.Sel.Name; s.funcs[k] != nil {
					li.calls[k] = true
				}
			}
			return true
		})
	}
	return li
}

// unconditional is the set of gates fn announces on however it returns: what
// its own top-level statements announce, plus what the functions those
// statements call announce the same way.
func (s *scan) unconditional(fn string) map[string]bool {
	if v, ok := s.unconditionalCache[fn]; ok {
		return v
	}
	out := map[string]bool{}
	// Stored before the walk, so a cycle ends here rather than recurring.
	s.unconditionalCache[fn] = out
	li := s.body[fn]
	if li == nil {
		return out
	}
	for g := range li.notifies {
		out[g] = true
	}
	for c := range li.calls {
		for g := range s.unconditional(c) {
			out[g] = true
		}
	}
	return out
}

// announcedAt reports whether a write cannot happen without gate g being
// announced: some statement list on the way to it announces g directly, or
// unconditionally calls something that does.
func (s *scan) announcedAt(w writeSite, g string) bool {
	for _, li := range w.reached {
		if li.notifies[g] {
			return true
		}
		for c := range li.calls {
			if s.unconditional(c)[g] {
				return true
			}
		}
	}
	return false
}

// walk records what one function does: the calls it makes, the guarded state
// it writes and reads, the gates it announces on and the gates it registers a
// waiter on.
func (s *scan) walk(key string, fn *ast.FuncDecl) {
	e := s.envFor(fn)
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch b := n.(type) {
		case *ast.BlockStmt:
			s.lists[b] = s.listInfoFor(e, b.List)
		case *ast.CaseClause:
			s.lists[b] = s.listInfoFor(e, b.Body)
		case *ast.CommClause:
			s.lists[b] = s.listInfoFor(e, b.Body)
		}
		return true
	})
	s.body[key] = s.lists[fn.Body]
	s.calls[key] = map[string]bool{}
	s.reads[key] = map[string]bool{}
	s.registers[key] = map[string]bool{}
	guarded := map[string]fieldDecl{}
	for _, f := range guardedFields {
		guarded[f.name()] = f
	}
	guardedNames := map[string]bool{}
	for _, f := range guardedFields {
		guardedNames[f.field] = true
	}
	gateByName := map[string]gateDecl{}
	for _, g := range parkGates {
		gateByName[g.name()] = g
	}
	// stack is the node path the traversal below is at, which is what says
	// which statement lists a write is inside.
	var stack []ast.Node
	reached := func() []*listInfo {
		var out []*listInfo
		for i := len(stack) - 1; i >= 0; i-- {
			if _, ok := stack[i].(*ast.FuncLit); ok {
				// Beyond a function literal the lists are the caller's, and
				// whoever calls the literal decides whether they were
				// entered.
				break
			}
			if li := s.lists[stack[i]]; li != nil {
				out = append(out, li)
			}
		}
		return out
	}
	note := func(field string, pos token.Pos, lowers bool, what string) {
		s.writes[key] = append(s.writes[key], writeSite{
			field: field, pos: s.fset.Position(pos), lowers: lowers, what: what, reached: reached(),
		})
	}
	// write records a write to whatever x names, and reports a field name it
	// could not attribute.
	write := func(x ast.Expr, pos token.Pos, lowers bool, what string) {
		attributed, name := s.selectorField(e, x)
		switch {
		case attributed != "":
			if _, ok := guarded[attributed]; ok {
				note(attributed, pos, lowers, what)
			}
		case guardedNames[name]:
			s.unattributed = append(s.unattributed, s.fset.Position(pos).String()+": "+key+" writes ."+name)
		}
	}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		stack = append(stack, n)
		switch st := n.(type) {
		case *ast.SelectorExpr:
			// Every mention of guarded state is a read unless the statement
			// cases below make it a write as well; a condition that reads a
			// counter is what a park depends on, and over-reporting a read
			// cannot hide a missing one.
			if attributed, _ := s.selectorField(e, st); attributed != "" {
				if _, ok := guarded[attributed]; ok {
					s.reads[key][attributed] = true
				}
			}
		case *ast.IncDecStmt:
			write(st.X, st.Pos(), st.Tok == token.DEC, st.Tok.String())
		case *ast.AssignStmt:
			for _, lhs := range st.Lhs {
				switch st.Tok {
				case token.SUB_ASSIGN:
					write(lhs, st.Pos(), true, "-=")
				case token.ADD_ASSIGN:
					write(lhs, st.Pos(), false, "+=")
				case token.ASSIGN:
					// A plain store says nothing about the direction, so it
					// counts as a fall.
					write(lhs, st.Pos(), true, "=")
				case token.DEFINE:
					// A define introduces a variable and writes no field.
				default:
					// Every other compound assignment -- *=, /=, &=, and the
					// rest -- can lower the value, so it counts as a fall.
					write(lhs, st.Pos(), true, st.Tok.String())
				}
			}
		case *ast.CallExpr:
			sel, ok := st.Fun.(*ast.SelectorExpr)
			if !ok {
				if id, ok := st.Fun.(*ast.Ident); ok && s.funcs["."+id.Name] != nil {
					s.calls[key]["."+id.Name] = true
				}
				return true
			}
			// An atomic on guarded state.
			switch sel.Sel.Name {
			case "Add":
				if len(st.Args) == 1 {
					write(sel.X, st.Pos(), lowersValue(st.Args[0]), "Add")
				}
			case "Store", "Swap":
				write(sel.X, st.Pos(), true, sel.Sel.Name)
			case "CompareAndSwap":
				if len(st.Args) == 2 {
					write(sel.X, st.Pos(), lowersValue(st.Args[1]), "CompareAndSwap")
				}
			}
			// An announcement on a gate, or a registration on one.
			if attributed, _ := s.selectorField(e, sel.X); attributed != "" {
				if g, ok := gateByName[attributed]; ok {
					if slices.Contains(g.registrars, sel.Sel.Name) {
						s.registers[key][attributed] = true
						if _, seen := s.parks[key]; !seen {
							s.parks[key] = s.fset.Position(st.Pos())
						}
					}
				}
			}
			// The gauge a gate reads to decide whether an announcement has
			// to do any work is the other half of a registration
			// (notifyGate.notify), so a wait that bumps it is registering
			// even before it takes the channel.
			if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "waiting" && sel.Sel.Name == "Add" {
				if attributed, _ := s.selectorField(e, inner.X); attributed != "" {
					if _, ok := gateByName[attributed]; ok && len(st.Args) == 1 && !lowersValue(st.Args[0]) {
						s.registers[key][attributed] = true
					}
				}
			}
			// A call to another function of this package.
			if owner := s.typeOf(e, sel.X); owner != "" {
				if k := owner + "." + sel.Sel.Name; s.funcs[k] != nil {
					s.calls[key][k] = true
				}
			}
		}
		return true
	})
}

// lowersValue reports whether an expression can lower the value it is applied
// to: a negation, a subtraction, or anything this scan cannot read.
func lowersValue(x ast.Expr) bool {
	switch t := x.(type) {
	case *ast.BasicLit:
		return false
	case *ast.Ident:
		return false
	case *ast.UnaryExpr:
		return t.Op == token.SUB
	case *ast.BinaryExpr:
		if t.Op == token.SUB {
			return true
		}
		if t.Op == token.ADD {
			return lowersValue(t.X) || lowersValue(t.Y)
		}
		return true
	case *ast.CallExpr:
		// A conversion of something, most often int64(n): read the thing.
		if len(t.Args) == 1 {
			return lowersValue(t.Args[0])
		}
	case *ast.ParenExpr:
		return lowersValue(t.X)
	}
	return true
}

// closure is the transitive union of what fn and everything it calls do.
func (s *scan) closure(fn string, of map[string]map[string]bool) map[string]bool {
	out := map[string]bool{}
	seen := map[string]bool{}
	var visit func(string)
	visit = func(k string) {
		if seen[k] {
			return
		}
		seen[k] = true
		for v := range of[k] {
			out[v] = true
		}
		for c := range s.calls[k] {
			visit(c)
		}
	}
	visit(fn)
	return out
}

// raises is the guarded state fn and everything it calls can raise, which is
// the state an admission books.
func (s *scan) raises(fn string) map[string]bool {
	out := map[string]bool{}
	seen := map[string]bool{}
	var visit func(string)
	visit = func(k string) {
		if seen[k] {
			return
		}
		seen[k] = true
		for _, w := range s.writes[k] {
			if !w.lowers {
				out[w.field] = true
			}
		}
		for c := range s.calls[k] {
			visit(c)
		}
	}
	visit(fn)
	return out
}

// callersOf returns every function of this package that calls fn.
func (s *scan) callersOf(fn string) []string {
	var out []string
	for k, cs := range s.calls {
		if cs[fn] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------------------
// The checks
// ---------------------------------------------------------------------------

// TestEveryGateIsDeclared holds parkGates to the package's own fields: a gate
// this file does not know about is one whose counters nothing below checks.
func TestEveryGateIsDeclared(t *testing.T) {
	s := loadScan(t)
	declared := map[string]bool{}
	for _, g := range parkGates {
		declared[g.name()] = true
		if _, ok := s.fields[g.owner]; !ok {
			t.Errorf("parkGates names %s and this package has no type %s", g.name(), g.owner)
			continue
		}
		if _, ok := s.fields[g.owner][g.field]; !ok {
			t.Errorf("parkGates names %s (%s) and %s has no such field", g.name(), g.why, g.owner)
		}
	}
	found := 0
	for owner, fields := range s.fields {
		for field, ftype := range fields {
			if ftype != "notifyGate" && ftype != "Strategy" {
				continue
			}
			found++
			if !declared[owner+"."+field] {
				t.Errorf("%s.%s is a %s and is not in parkGates: a gate nothing has declared is a "+
					"gate whose counters TestEveryFallInGuardedStateIsAnnounced does not check",
					owner, field, ftype)
			}
		}
	}
	if found == 0 {
		t.Fatal("the scan found no gate field at all, so this check cannot fail: " +
			"notifyGate and wait.Strategy fields were not recognised")
	}
	for _, f := range guardedFields {
		if _, ok := s.fields[f.owner][f.field]; !ok {
			t.Errorf("guardedFields names %s and this package has no such field", f.name())
		}
		for _, g := range f.gates {
			if !declared[g] {
				t.Errorf("%s names gate %s, which is not in parkGates", f.name(), g)
			}
		}
	}
}

// TestEveryFallInGuardedStateIsAnnounced is the check the family exists for.
//
// Every write that can lower a counter somebody's park condition reads -- and
// every write at all to state that is replaced rather than counted -- must be
// in a function that announces the change on every gate that guards it,
// itself or through something it calls. The four incidents this file's
// preamble names are each exactly one such write: a withdrawal path that gave
// a permit back and did not announce it, beside an ordinary release path that
// did.
//
// A write that legitimately announces nothing goes in silentWrites with its
// reason, and an entry there that is no longer needed fails too: a list of
// exemptions nobody prunes is how a check of this shape stops meaning
// anything.
func TestEveryFallInGuardedStateIsAnnounced(t *testing.T) {
	s := loadScan(t)
	guarded := map[string]fieldDecl{}
	for _, f := range guardedFields {
		guarded[f.name()] = f
	}
	silent := map[string]silentDecl{}
	for _, d := range silentWrites {
		silent[d.key()] = d
	}
	used := map[string]bool{}
	written := map[string]bool{}
	sites := 0
	for _, fn := range sortedKeys(keysOf(s.writes)) {
		for _, w := range s.writes[fn] {
			decl := guarded[w.field]
			written[w.field] = true
			if !w.lowers && !decl.anyWrite {
				continue
			}
			sites++
			var missing []string
			for _, g := range decl.gates {
				if !s.announcedAt(w, g) {
					missing = append(missing, g)
				}
			}
			if len(missing) == 0 {
				continue
			}
			verb := "lowers"
			if decl.anyWrite {
				verb = "replaces"
			}
			d, exempt := silent[fn+" -> "+w.field]
			if !exempt {
				t.Errorf("%s: %s (%s) %s %s and announces nothing on %s\n"+
					"    %s\n"+
					"    A permit somebody parks on, given back on a path that skips the notification "+
					"its ordinary counterpart makes, is this repository's most repeated defect: "+
					"dispatch.gate, lane.reserve, Engine.reserveTotal and pendingBound.admit were all "+
					"this. Route the write through the function that announces it, or add it to "+
					"silentWrites with the reason.",
					w.pos, fn, w.what, verb, w.field, strings.Join(missing, ", "), decl.why)
				continue
			}
			used[d.key()] = true
			if !d.viaCallers {
				continue
			}
			callers := s.callersOf(fn)
			if len(callers) == 0 {
				t.Errorf("%s: %s %s %s and silentWrites says %q, and it has no caller in this "+
					"package to delegate to", w.pos, fn, verb, w.field, d.why)
				continue
			}
			for _, c := range callers {
				cn := s.unconditional(c)
				for _, g := range decl.gates {
					if !cn[g] {
						t.Errorf("%s: %s %s %s and silentWrites says %q, and %s does not announce "+
							"it on %s", w.pos, fn, verb, w.field, d.why, c, g)
					}
				}
			}
		}
	}
	if sites == 0 {
		t.Fatal("the scan found no write to guarded state at all, so this check cannot fail")
	}
	for _, f := range guardedFields {
		if !written[f.name()] {
			t.Errorf("nothing in this package writes %s: either the field is gone or the scan no "+
				"longer attributes its writes, and either way this check is not checking it", f.name())
		}
	}
	for _, d := range silentWrites {
		if !used[d.key()] {
			t.Errorf("silentWrites carries %s and nothing needs it any more: delete the entry", d.key())
		}
	}
}

// TestEveryParkIsOnTheWholeCondition is the other half of the pair: a park
// that is woken by every change it needs but re-checks less than its own
// admission books returns at once, is refused again and spins. That is what
// Topic.waitRoom did with the engine-wide limit , and it
// costs a processor rather than a wake, which is why no test noticed.
//
// For every park it holds three things to the source: the gates it registers
// on are the ones it declares, the conditions it declares are ones it
// actually calls and they read the state it declares, and everything the
// admission it retries books is state that condition reads.
func TestEveryParkIsOnTheWholeCondition(t *testing.T) {
	s := loadScan(t)
	guarded := map[string]fieldDecl{}
	for _, f := range guardedFields {
		guarded[f.name()] = f
	}
	declared := map[string]bool{}
	for i := range parkCases {
		declared[parkCases[i].fn] = true
	}
	if len(s.parks) == 0 {
		t.Fatal("the scan found no park at all, so this check cannot fail: the gate registrars " +
			"were not recognised")
	}
	for _, fn := range sortedKeys(keysOf(s.parks)) {
		if !declared[fn] {
			t.Errorf("%s: %s registers on a gate and is not in parkCases: a park nothing measures is "+
				"how the per-lane wait came to re-check half its condition ",
				s.parks[fn], fn)
		}
	}
	for i := range parkCases {
		p := &parkCases[i]
		fn := s.funcs[p.fn]
		if fn == nil {
			t.Errorf("parkCases names %s and this package has no such function", p.fn)
			continue
		}
		if _, ok := s.parks[p.fn]; !ok {
			t.Errorf("%s registers on no gate and parkCases says it parks (%s)", p.fn, p.why)
			continue
		}
		// The gates, in both directions.
		on := s.registers[p.fn]
		for _, g := range p.gates {
			if !on[g] {
				t.Errorf("%s declares that it parks on %s and does not register on it", p.fn, g)
			}
		}
		for _, g := range sortedKeys(on) {
			if !slices.Contains(p.gates, g) {
				t.Errorf("%s registers on %s and parkCases does not declare it", p.fn, g)
			}
		}
		// The conditions it declares are ones it calls, and the state it
		// declares is state they read.
		reads := map[string]bool{}
		for _, c := range p.conditions {
			if s.funcs[c] == nil {
				t.Errorf("%s declares condition %s and this package has no such function", p.fn, c)
				continue
			}
			if !s.calls[p.fn][c] {
				t.Errorf("%s declares that it parks on %s and does not call it", p.fn, c)
				continue
			}
			for f := range s.closure(c, s.reads) {
				reads[f] = true
			}
		}
		// And the other way round: guarded state the condition reads and the
		// declaration does not name. It is the direction that catches a
		// condition growing a term rather than losing one -- a lane's
		// hasRoom that started reading the engine's count would otherwise
		// pass with the old declaration.
		for _, f := range sortedKeys(reads) {
			if p.impure != "" || slices.Contains(p.fields, f) {
				continue
			}
			t.Errorf("%s parks on %v and its condition (%v) reads %s, which parkCases does not "+
				"declare: declare it, or stop reading it in the condition", p.fn, p.gates, p.conditions, f)
		}
		for _, f := range p.fields {
			decl, ok := guarded[f]
			if !ok {
				t.Errorf("%s declares field %s, which is not in guardedFields", p.fn, f)
				continue
			}
			if !reads[f] {
				t.Errorf("%s declares that its condition depends on %s and none of %v reads it",
					p.fn, f, p.conditions)
			}
			shared := false
			for _, g := range decl.gates {
				if slices.Contains(p.gates, g) {
					shared = true
				}
			}
			if !shared {
				t.Errorf("%s parks on %v and its condition depends on %s, which is announced on %v: "+
					"a change in it can never wake this park", p.fn, p.gates, f, decl.gates)
			}
		}
		// And the whole of it: everything the retried admission books.
		if p.retries == "" {
			continue
		}
		if s.funcs[p.retries] == nil {
			t.Errorf("%s declares that it retries %s and this package has no such function", p.fn, p.retries)
			continue
		}
		for _, f := range sortedKeys(s.raises(p.retries)) {
			if slices.Contains(p.fields, f) {
				continue
			}
			t.Errorf("%s (%s) retries %s, which books %s, and its condition (%v) does not read it\n"+
				"    A park that re-checks less than its admission needs is woken, finds the half it "+
				"looks at satisfied, retries, is refused by the half it does not and spins: it burns a "+
				"processor rather than losing a wake, which is why no test notices.",
				p.fn, p.why, p.retries, f, p.conditions)
		}
	}
}

// TestTheScanAttributesEveryWrite is the scan's own coverage check: a write to
// guarded state through an expression it cannot resolve is a write it cannot
// hold to anything, and a check that silently skips its subject is worth
// nothing.
func TestTheScanAttributesEveryWrite(t *testing.T) {
	s := loadScan(t)
	for _, u := range s.unattributed {
		t.Errorf("%s: the park/notify scan could not attribute this write to a type, so it cannot "+
			"tell whether it is a write to guarded state (parkguard_test.go: scan.typeOf)", u)
	}
}

func keysOf[V any](m map[string]V) map[string]bool {
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}
