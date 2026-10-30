package cloudevents

import (
	"strings"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/types"

	"github.com/iqhive/iqmq/codec"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Header is one attribute header of a binary-mode message: the portable
// lower-case name and its value.
//
// It is deliberately NOT mq.Header. This module must not import the root
// module's message types to hand a caller a header list -- the root's Headers
// representation is still a bake-off and binding this package to
// one shape would make it depend on which kind wins. The caller copies these
// into whatever Headers is on the day, which is two lines at the call site and
// zero coupling here.
type Header struct {
	// Name is the header name, prefix included, lower-case.
	Name string
	// Value is the attribute's canonical string form.
	Value string
}

// Binding is CloudEvents BINARY content mode: context attributes in transport
// headers, data alone in the body.
//
// It is not a codec.Codec, and it cannot be: codec.Codec returns a body and
// nothing else, while binary mode's whole point is that the attributes travel
// beside the body where a broker's own filtering and routing can see them. See
// the package comment and.
//
// Binding is stateless and safe for concurrent use.
type Binding struct {
	prefix   string
	limit    int
	maxAttrs int
	validate bool
}

// NewBinding returns the binary-mode binding with the default configuration.
func NewBinding() Binding { return NewBindingWithConfig(Config{}) }

// NewBindingWithConfig returns the binary-mode binding configured by c.
func NewBindingWithConfig(c Config) Binding {
	return Binding{
		// Lower-cased because Decode lower-cases every header name before it
		// compares, and Header.Name is lower-case by contract: a "CE-" prefix
		// kept as given wrote headers no Decode of the same Binding matched.
		prefix:   lower(c.prefix()),
		limit:    c.maxBytes(),
		maxAttrs: c.maxAttributes(),
		validate: !c.SkipValidation,
	}
}

// Prefix returns the attribute header prefix in force.
func (b Binding) Prefix() string { return b.prefix }

// Encode splits an event into the body a publisher sends and the headers it
// sends beside it.
//
// The returned body is the event's DATA, unencoded: binary mode does not wrap
// it. headers carries every context attribute and extension, plus
// `content-type` set to the data's own media type, which is what the
// CloudEvents HTTP binding requires of a binary-mode message.
//
// The body aliases the event's data and is not copied. A publisher that owns
// its buffer (the specification) may pass it straight through; one that mutates the event
// afterwards must copy.
func (b Binding) Encode(e *event.Event) (body []byte, headers []Header, err error) {
	if e == nil {
		return nil, nil, ErrNotEvent
	}
	if b.validate {
		if verr := e.Validate(); verr != nil {
			return nil, nil, errcode.New(errcode.InvalidArgument, "codec/cloudevents: "+verr.Error())
		}
	}
	if e.SpecVersion() != SpecVersion {
		return nil, nil, errcode.New(errcode.InvalidArgument, "codec/cloudevents: specversion "+e.SpecVersion()+" is not "+SpecVersion)
	}
	if xerr := checkExtensions(e); xerr != nil {
		return nil, nil, errcode.New(errcode.InvalidArgument, "codec/cloudevents: "+xerr.Error())
	}
	ext := e.Extensions()
	headers = make([]Header, 0, 8+len(ext))
	// A value a transport header cannot hold unchanged is refused rather than
	// trimmed: see ErrUnrepresentable.
	var bad string
	add := func(name, value string) {
		if value == "" {
			return
		}
		if strings.TrimSpace(value) != value {
			bad = name
			return
		}
		headers = append(headers, Header{Name: b.prefix + name, Value: value})
	}
	add("specversion", e.SpecVersion())
	add("id", e.ID())
	add("source", e.Source())
	add("type", e.Type())
	add("subject", e.Subject())
	if t := e.Time(); !t.IsZero() {
		add("time", t.UTC().Format(time.RFC3339Nano))
	}
	add("dataschema", e.DataSchema())
	if ct := e.DataContentType(); ct != "" {
		add("datacontenttype", ct)
		// the portable content-type header, which in binary mode is the
		// DATA's type and not a CloudEvents type.
		headers = append(headers, Header{Name: codec.HeaderContentType, Value: ct})
	}
	// Extensions in sorted order, so the header block is a function of the
	// event: the specification requires deterministic encoding and Go's map iteration is
	// not one.
	names := make([]string, 0, len(ext))
	for name := range ext {
		names = append(names, lower(name))
	}
	sortStrings(names)
	for _, name := range names {
		s, serr := types.Format(ext[name])
		if serr != nil {
			// A value the CloudEvents type system cannot format is the
			// caller's bug, not a wire condition.
			return nil, nil, errcode.New(errcode.InvalidArgument,
				"codec/cloudevents: extension "+name+": "+serr.Error())
		}
		if s == "" {
			// add skips an empty value because an unset optional attribute
			// reads as one, but an extension in the map is SET: dropping it
			// gave binary mode a different event than structured mode, which
			// carries "name":"" faithfully.
			headers = append(headers, Header{Name: b.prefix + name})
			continue
		}
		add(name, s)
	}
	if bad != "" {
		return nil, nil, errcode.New(errcode.InvalidArgument,
			"codec/cloudevents: attribute "+bad+" has leading or trailing whitespace, which binary mode cannot carry")
	}
	return e.Data(), headers, nil
}

// PartitionKey returns the event's CloudEvents Partitioning extension, which
// is the attribute iqmq's partition key (the specification) maps to and from. It is the one
// two-way mapping between the two metadata models, and it is a function of its
// own so that a publisher can set mq.Key from it without re-deriving the
// mapping.
func PartitionKey(e *event.Event) (string, bool) {
	if e == nil {
		return "", false
	}
	v, ok := e.Extensions()[PartitionKeyExtension]
	if !ok {
		return "", false
	}
	s, err := types.Format(v)
	if err != nil {
		return "", false
	}
	return s, s != ""
}

// SetPartitionKey sets the Partitioning extension, for a publisher that has an
// iqmq key and wants the event to carry it too.
func SetPartitionKey(e *event.Event, key string) {
	if e == nil || key == "" {
		return
	}
	e.SetExtension(PartitionKeyExtension, key)
}

// Decode rebuilds an event from a delivery's body and headers.
//
// The limits are applied BEFORE anything is built: MaxBytes against the
// body, and MaxAttributes against the number of prefixed headers, because a
// delivery with a hundred thousand `ce-x` headers would otherwise build a
// hundred thousand extensions on the receive path and the SENDER chooses how
// many there are.
//
// A delivery with no prefixed header at all is ErrNoAttributes rather than an
// empty event: a body that was never a CloudEvent must not become one. For the
// same reason one without a specversion header is ErrSpecVersion, as a
// structured body without one is.
//
// The event's data is a copy of body, as codec's Unmarshal retains nothing it
// was given. A delivery's body is the consumer's to recycle once the delivery
// is settled under ConsumerConfig.BufferReuse, and an event that shared it
// changed under its holder when the next delivery was received.
func (b Binding) Decode(body []byte, headers []Header, out *event.Event) error {
	if out == nil {
		return codec.ErrNilTarget
	}
	if b.limit >= 0 && len(body) > b.limit {
		return ErrTooLarge
	}
	// Counted before any attribute is parsed and before the event is touched.
	prefixed := 0
	for i := range headers {
		if strings.HasPrefix(lower(headers[i].Name), b.prefix) {
			prefixed++
		}
	}
	if prefixed == 0 {
		return &codec.DecodeError{ContentType: ContentType, Err: ErrNoAttributes}
	}
	if b.maxAttrs >= 0 && prefixed > b.maxAttrs {
		return ErrTooManyAttributes
	}

	e := event.New(SpecVersion)
	dataContentType, contentType := "", ""
	sawSpecVersion := false
	for _, h := range headers {
		name := lower(h.Name)
		if !strings.HasPrefix(name, b.prefix) {
			if name == codec.HeaderContentType {
				contentType = h.Value
			}
			continue
		}
		switch attr := name[len(b.prefix):]; attr {
		case "specversion":
			if h.Value != SpecVersion {
				return &codec.DecodeError{ContentType: ContentType, Err: ErrSpecVersion}
			}
			sawSpecVersion = true
		case "id":
			e.SetID(h.Value)
		case "source":
			e.SetSource(h.Value)
		case "type":
			e.SetType(h.Value)
		case "subject":
			e.SetSubject(h.Value)
		case "time":
			t, terr := types.ParseTime(h.Value)
			if terr != nil {
				return &codec.DecodeError{ContentType: ContentType, Err: terr}
			}
			e.SetTime(t)
		case "dataschema":
			e.SetDataSchema(h.Value)
		case "datacontenttype":
			dataContentType = h.Value
		default:
			if attr == "" {
				return &codec.DecodeError{ContentType: ContentType, Err: ErrInvalidEvent}
			}
			if reservedAttributes[attr] {
				// `ce-data` is not an extension called data; it is a sender
				// that does not understand binary mode.
				return &codec.DecodeError{ContentType: ContentType, Err: ErrReservedExtension}
			}
			e.SetExtension(attr, h.Value)
		}
	}
	if !sawSpecVersion {
		// specversion is REQUIRED, and event.New filled it in above, so
		// without this a delivery that never claimed to be a 1.0 event --
		// structured mode refuses the same event as a body -- became one.
		return &codec.DecodeError{ContentType: ContentType, Err: ErrSpecVersion}
	}
	if dataContentType == "" {
		// The bindings map datacontenttype to the transport's content-type
		// and define no ce-datacontenttype header, so a conforming sender's
		// data type is only there.
		dataContentType = contentType
	}
	if dataContentType != "" {
		e.SetDataContentType(dataContentType)
	}
	if len(body) > 0 {
		e.DataEncoded = append([]byte(nil), body...)
	}
	if b.validate {
		if verr := e.Validate(); verr != nil {
			return &codec.DecodeError{ContentType: ContentType, Err: ErrInvalidEvent}
		}
	}
	*out = e
	return nil
}

// sortStrings is an insertion sort: an event has a handful of extensions, and
// sort.Strings pulls in the reflect-based sort machinery for a slice this
// small.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
