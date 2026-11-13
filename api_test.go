package iqmq_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestTheAPIBaselinesNameNoFileSystemPath holds every hack/api/*.export to
// file names relative to a module, as `hack/apidiff.sh -w` writes them with
// -trimpath. A baseline written without it records the absolute path of the
// checkout and the module cache that wrote it, so a rewrite from any other
// directory changed the whole file, and every committed baseline published
// the writer's local paths.
//
// A baseline this test cannot parse, or one in which it finds no file name at
// all, is a format it no longer reads, and fails rather than passing for want
// of a match.
func TestTheAPIBaselinesNameNoFileSystemPath(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("hack", "api", "*.export"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no baseline under hack/api")
	}
	for _, p := range paths {
		names, err := baselineFileNames(p)
		if err != nil {
			t.Errorf("%s: %v", p, err)
			continue
		}
		if len(names) == 0 {
			t.Errorf("%s: no file name found, so this test cannot read the format", p)
			continue
		}
		var abs []string
		for _, n := range names {
			if absoluteFileName(n) {
				abs = append(abs, n)
			}
		}
		if len(abs) > 0 {
			t.Errorf("%s names %d of its %d file(s) by an absolute path, e.g. %s: rewrite it with `make apidiff-baseline`",
				p, len(abs), len(names), abs[0])
		}
	}
}

// absoluteFileName reports whether a file name a baseline records is an
// absolute path on the machine that wrote it: "/..." on Unix, a drive letter
// and a colon ("C:\..." or "C:/...") on Windows. A trimmed name starts with a
// module or package path instead.
func absoluteFileName(n string) bool {
	if strings.HasPrefix(n, "/") {
		return true
	}
	return len(n) > 2 && n[1] == ':' && (n[2] == '\\' || n[2] == '/')
}

// baselineFileNames returns the strings ending in ".go" that an apidiff
// baseline's export data holds, the file names among them. The baseline is a
// zip with one member per package (its `module` member holds the module path)
// in gcexportdata.Write's format: "i", then uvarints for the format version,
// the string section's length and the data's, then the string section, each
// string a uvarint length and its bytes.
func baselineFileNames(path string) ([]string, error) {
	z, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = z.Close() }()
	var names []string
	for _, f := range z.File {
		if f.Name == "module" {
			continue
		}
		r, err := f.Open()
		if err != nil {
			return nil, err
		}
		b, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil {
			return nil, err
		}
		if len(b) == 0 || b[0] != 'i' {
			return nil, fmt.Errorf("member %s is not export data in the indexed format", f.Name)
		}
		rd := bytes.NewReader(b[1:])
		var hdr [3]uint64 // version, string section, data
		for i := range hdr {
			if hdr[i], err = binary.ReadUvarint(rd); err != nil {
				return nil, fmt.Errorf("member %s: reading its header: %w", f.Name, err)
			}
		}
		off := len(b) - rd.Len()
		if hdr[1] > uint64(rd.Len()) {
			return nil, fmt.Errorf("member %s: a string section of %d bytes in %d", f.Name, hdr[1], rd.Len())
		}
		strs := bytes.NewReader(b[off : off+int(hdr[1])])
		for strs.Len() > 0 {
			n, err := binary.ReadUvarint(strs)
			if err != nil || n > uint64(strs.Len()) {
				return nil, fmt.Errorf("member %s: a string that overruns its section", f.Name)
			}
			s := make([]byte, n)
			_, _ = io.ReadFull(strs, s)
			if bytes.HasSuffix(s, []byte(".go")) {
				names = append(names, string(s))
			}
		}
	}
	return names, nil
}

// TestPublicSurfaceNamesNoInternalType holds the one property a frozen API
// cannot be allowed to lose quietly: no exported declaration mentions a type
// from internal/.
//
// A method such as `func (e *ControlError) Code() errcode.Code` compiles, and
// go doc renders it, and a caller outside the module cannot write down its
// result type nor implement an interface containing it. It is also copied
// verbatim into hack/api/root.export, so the baseline freezes the internal
// package's name into the public contract. The specification shipped exactly that defect
// once and found it by reading go doc; this is the check that finds the next
// one.
func TestPublicSurfaceNamesNoInternalType(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		// The local names the file gives to internal packages.
		internalNames := map[string]string{}
		for _, imp := range f.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if !strings.Contains(path, "/internal/") {
				continue
			}
			local := path[strings.LastIndex(path, "/")+1:]
			if imp.Name != nil {
				local = imp.Name.Name
			}
			internalNames[local] = path
		}
		if len(internalNames) == 0 {
			continue
		}
		ast.Inspect(f, func(n ast.Node) bool {
			var sig ast.Node
			var what string
			switch d := n.(type) {
			case *ast.FuncDecl:
				if !d.Name.IsExported() {
					return true
				}
				if d.Recv != nil && !exportedReceiver(d.Recv) {
					return true
				}
				sig, what = d.Type, "func "+d.Name.Name
			case *ast.TypeSpec:
				if !d.Name.IsExported() {
					return true
				}
				// A struct's unexported fields are not part of the surface:
				// Headers stores internal/payload.KV precisely so that the
				// engine boundary is free, and that is the point of the specification's
				// winner. Only the exported fields are checked.
				if st, ok := d.Type.(*ast.StructType); ok {
					checkExportedFields(t, name, d.Name.Name, st, internalNames)
					return true
				}
				sig, what = d.Type, "type "+d.Name.Name
			case *ast.ValueSpec:
				anyExported := false
				for _, id := range d.Names {
					if id.IsExported() {
						anyExported = true
					}
				}
				if !anyExported || d.Type == nil {
					return true
				}
				sig, what = d.Type, "var/const "+d.Names[0].Name
			default:
				return true
			}
			ast.Inspect(sig, func(m ast.Node) bool {
				sel, ok := m.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				id, ok := sel.X.(*ast.Ident)
				if !ok {
					return true
				}
				if path, isInternal := internalNames[id.Name]; isInternal {
					// An exported field of an exported struct is reached
					// through the TypeSpec above, so this covers fields too.
					t.Errorf("%s: %s names %s.%s, from the internal package %s.\n"+
						"  A caller outside the module cannot write that type down, and "+
						"hack/api/root.export freezes the internal package's name into the "+
						"public contract. Declare the type in the root package instead.",
						name, what, id.Name, sel.Sel.Name, path)
				}
				return true
			})
			return true
		})
	}
}

// checkExportedFields reports an internal type named by an exported field of
// an exported struct.
func checkExportedFields(t *testing.T, file, typeName string, st *ast.StructType, internalNames map[string]string) {
	t.Helper()
	for _, field := range st.Fields.List {
		exported := len(field.Names) == 0 // an embedded field
		for _, id := range field.Names {
			if id.IsExported() {
				exported = true
			}
		}
		if !exported {
			continue
		}
		ast.Inspect(field.Type, func(m ast.Node) bool {
			sel, ok := m.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			id, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			if path, isInternal := internalNames[id.Name]; isInternal {
				t.Errorf("%s: field %s of type %s names %s.%s, from the internal package %s.",
					file, field.Names, typeName, id.Name, sel.Sel.Name, path)
			}
			return true
		})
	}
}

// exportedReceiver reports whether a method's receiver type is exported.
func exportedReceiver(recv *ast.FieldList) bool {
	if len(recv.List) == 0 {
		return false
	}
	expr := recv.List[0].Type
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if idx, ok := expr.(*ast.IndexExpr); ok {
		expr = idx.X
	}
	id, ok := expr.(*ast.Ident)
	return ok && id.IsExported()
}

// frozenA27 is the half of docs/api-freeze.md written down as a table of
// types: one field per frozen signature.
//
// Filling a field with a method value requires the two function types to be
// IDENTICAL — a changed parameter, result or variadic tail does not compile —
// which is why the freeze is checked here as well as by the apidiff baseline.
// The baseline says what the surface IS; this says what the freeze DECIDED,
// and they are different documents.
type frozenA27 struct {
	// Delivery and settlement.
	ack     func() error
	nack    func(...mq.NackOption) error
	reject  func() error
	settled func() bool
	// The accessors the specification states and added to the freeze: the metadata,
	// the escape hatch in both its forms, the lease extension and the
	// escape from borrowed storage. Extend takes no context, because the
	// settlement verbs beside it do not either — the handle carries the
	// context every driver call is bounded by.
	deliveryInfo   func() mq.DeliveryInfo
	deliveryNative func() any
	nativeAs       func(*mq.Delivery) (*mq.Message, bool)
	extend         func(time.Duration) error
	cloneMessage   func() *mq.Message
	// The delivery's own shape: an embedded Message by value and
	// the attempt beside it.
	message mq.Message
	attempt int

	// Handles.
	newClient    func(mq.Backend, ...mq.ClientOption) *mq.Client
	publisher    func(context.Context, mq.PublisherConfig) (*mq.PublisherHandle, error)
	consumer     func(context.Context, mq.ConsumerConfig) (*mq.ConsumerHandle, error)
	clientPub    func(context.Context, string, *mq.Message, ...mq.PublishOption) error
	info         func() mq.DriverInfo
	capabilities func() mq.Capabilities
	admin        func() (mq.Admin, error)
	native       func() any
	ping         func(context.Context) error
	health       func() mq.HealthSnapshot
	shutdown     func(context.Context) error
	closeClient  func() error
	publish      func(context.Context, *mq.Message, ...mq.PublishOption) error
	receive      func(context.Context, []mq.Delivery) (int, error)
}

// bound reports whether every signature in the table was filled in, so that
// the table is read as well as written.
func (f frozenA27) bound() bool {
	return f.ack != nil && f.nack != nil && f.reject != nil && f.settled != nil &&
		f.deliveryInfo != nil && f.deliveryNative != nil && f.nativeAs != nil &&
		f.extend != nil && f.cloneMessage != nil &&
		f.newClient != nil && f.publisher != nil && f.consumer != nil && f.clientPub != nil &&
		f.info != nil && f.capabilities != nil && f.admin != nil && f.native != nil &&
		f.ping != nil && f.health != nil && f.shutdown != nil && f.closeClient != nil &&
		f.publish != nil && f.receive != nil &&
		f.message.Topic == "orders" && f.attempt == 1
}

// TestFrozenSignaturesA27 fills the table in.
func TestFrozenSignaturesA27(t *testing.T) {
	var (
		d  *mq.Delivery
		cl *mq.Client
		p  *mq.PublisherHandle
		c  *mq.ConsumerHandle
	)
	dv := mq.Delivery{Message: mq.Message{Topic: "orders"}, Attempt: 1}
	f := frozenA27{
		ack:     d.Ack,
		nack:    d.Nack,
		reject:  d.Reject,
		settled: d.Settled,
		message: dv.Message,
		attempt: dv.Attempt,

		deliveryInfo:   d.Info,
		deliveryNative: d.Native,
		nativeAs:       mq.NativeAs[*mq.Message],
		extend:         d.Extend,
		cloneMessage:   d.CloneMessage,

		newClient:    mq.NewClient,
		publisher:    cl.Publisher,
		consumer:     cl.Consumer,
		clientPub:    cl.Publish,
		info:         cl.Info,
		capabilities: cl.Capabilities,
		admin:        cl.Admin,
		native:       cl.Native,
		ping:         cl.Ping,
		health:       cl.Health,
		shutdown:     cl.Shutdown,
		closeClient:  cl.Close,
		publish:      p.Publish,
		receive:      c.Receive,
	}
	if !f.bound() {
		t.Fatalf("the frozen signatures did not bind: %+v", dv)
	}

	// The SPI: the mandatory surface, the optional batch sink, and the specification's
	// narrow interface beside the concrete handle.
	var (
		_ mq.Backend   = (*fakeBroker)(nil)
		_ mq.Sink      = (*fakeSink)(nil)
		_ mq.BatchSink = (*fakeBatchSink)(nil)
		_ mq.Source    = (*fakeSource)(nil)
		_ mq.Settler   = (*fakeSettler)(nil)
		_ mq.Publisher = (*mq.PublisherHandle)(nil)
	)
}
