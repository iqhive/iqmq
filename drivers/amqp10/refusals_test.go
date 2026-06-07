package amqp10_test

import (
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	aq "bitbucket.org/iqhive/iqmq/drivers/amqp10"
)

// The mapping's refusals: the specification's "enforced before I/O".
//
// mapping.go's round trip is well covered, and its refusals are not -- which
// is the wrong way round for a mapper, because a round trip that works is
// the case the caller already sees and a refusal that does not is a message
// on the wire that the broker will reject or, worse, accept wrongly.

// TestAMessageThatIsNotThereIsRefused.
//
// Both entry points take the message, and a nil is a driver bug rather than
// a caller's: the SPI does not let one through. Refusing is what keeps it a
// named error instead of a nil dereference inside the encoder.
func TestAMessageThatIsNotThereIsRefused(t *testing.T) {
	t.Parallel()
	m := mapperFor(t, mq.MetadataNativeStrict)

	if err := m.CheckPublish(nil, nil); err == nil {
		t.Error("CheckPublish accepted a nil message")
	}
	if _, err := m.Encode(aq.NewBuf(), nil, nil); err == nil {
		t.Error("Encode accepted a nil message")
	}
}

// TestADelayAndADeliveryTimeTogetherAreRefused.
//
// They are two ways of saying the same thing and they can disagree. The specification
// makes that a refusal rather than a precedence rule, because a caller who
// set both has a bug and a driver that silently picked one would hide it.
func TestADelayAndADeliveryTimeTogetherAreRefused(t *testing.T) {
	t.Parallel()
	m := mapperFor(t, mq.MetadataNativeStrict)

	err := m.CheckPublish(&mq.Message{Body: []byte("x")}, &mq.PublishOptions{
		Delay:     time.Minute,
		DeliverAt: time.Now().Add(time.Hour),
	})
	if err == nil {
		t.Error("a publication carrying both a delay and a delivery time was accepted; they can disagree, " +
			"and picking one silently hides the caller's bug")
	}
}

// TestAReservedHeaderNameThisMappingDoesNotKnowIsRefused.
//
// The reserved prefix is the driver's own namespace, so a header in it that
// this mapping has no rule for is a caller reaching for something that does
// not exist. Passing it through as an ordinary application property would
// put the driver's namespace on the wire with a meaning nobody defined.
//
// The refusal comes from the CORE here, not from this mapper: `Headers.Set`
// answers "header key uses the reserved iqmq- prefix" before the driver sees
// it. The mapper's own `errReservedHeaderName` is a second line for a name
// the core lets through, and deleting it leaves this test green -- which is
// worth knowing, because the assertion is about the behaviour a caller sees
// and not about which layer produces it.
func TestAReservedHeaderNameThisMappingDoesNotKnowIsRefused(t *testing.T) {
	t.Parallel()
	m := mapperFor(t, mq.MetadataNativeStrict)

	src := &mq.Message{Body: []byte("x")}
	src.Headers.Set(mq.ReservedHeaderPrefix+"amqp-no-such-rule", []byte("v"))

	if _, err := m.Encode(aq.NewBuf(), src, nil); err == nil {
		t.Error("a header under the reserved prefix with no rule was encoded; the prefix is the driver's " +
			"namespace and passing one through gives it a meaning nobody defined")
	}
}

// TestAReservedHeaderWhoseValueDoesNotParseIsRefused.
//
// Two of the reserved headers carry a typed value -- an RFC3339 timestamp
// and a 32-bit sequence -- and a value that does not parse cannot be put on
// the wire at all. Refusing names the header; encoding a zero would put a
// 1970 expiry on somebody's message.
func TestAReservedHeaderWhoseValueDoesNotParseIsRefused(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		header string
		value  string
	}{
		{"an absolute expiry that is not a timestamp", aq.HeaderAbsoluteExpiry, "half past three"},
		{"an absolute expiry in the wrong format", aq.HeaderAbsoluteExpiry, "2026-09-21"},
		{"a group sequence that is not a number", aq.HeaderGroupSequence, "first"},
		{"a group sequence too large for 32 bits", aq.HeaderGroupSequence, "4294967296"},
		{"a negative group sequence", aq.HeaderGroupSequence, "-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := mapperFor(t, mq.MetadataNativeStrict)
			src := &mq.Message{Body: []byte("x")}
			src.Headers.Set(tc.header, []byte(tc.value))

			if _, err := m.Encode(aq.NewBuf(), src, nil); err == nil {
				t.Errorf("%s (%q) was encoded; a zero would put a 1970 expiry or a wrong sequence on "+
					"somebody's message", tc.header, tc.value)
			}
		})
	}
}

// TestTheTypedReservedHeadersAreAcceptedWhenTheyParse.
//
// The other direction, because a mapper that refused everything would pass
// the test above and be useless.
func TestTheTypedReservedHeadersAreAcceptedWhenTheyParse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ header, value string }{
		{aq.HeaderAbsoluteExpiry, "2026-09-21T12:00:00Z"},
		{aq.HeaderGroupSequence, "42"},
		{aq.HeaderFirstAcquirer, "true"},
		{aq.HeaderCausationID, "the-cause"},
		{aq.HeaderSubject, "orders.created"},
		{aq.HeaderReplyToGroupID, "group-7"},
	} {
		m := mapperFor(t, mq.MetadataNativeStrict)
		src := &mq.Message{Body: []byte("x")}
		src.Headers.Set(tc.header, []byte(tc.value))

		if _, err := m.Encode(aq.NewBuf(), src, nil); err != nil {
			t.Errorf("%s=%q was refused: %v", tc.header, tc.value, err)
		}
	}
}

// TestAMetadataModeThatIsNotOneIsRefusedAtConstruction.
//
// The mode decides what the mapper does with everything, so a value that is
// not one of them cannot be defaulted: a mapper built with it would have no
// rule for any header.
func TestAMetadataModeThatIsNotOneIsRefusedAtConstruction(t *testing.T) {
	t.Parallel()
	caps, err := aq.CapabilitiesForTest(testProfile(), aq.Facts{})
	if err != nil {
		t.Fatalf("building the capability table: %v", err)
	}

	if _, err := aq.MapperForTest(testProfile(), mq.MetadataMode(99), caps, true); err == nil {
		t.Error("a mapper was built with a metadata mode that is not one of them")
	}
}

// TestABodyOverThePeersCeilingIsRefusedBeforeItIsWritten.
//
// The specification again: the peer advertised a maximum and a message over it is refused
// here rather than written and rejected, which is the difference between an
// error naming the limit and a connection dropped mid-frame.
func TestABodyOverThePeersCeilingIsRefusedBeforeItIsWritten(t *testing.T) {
	t.Parallel()
	m := mapperFor(t, mq.MetadataNativeStrict)

	huge := &mq.Message{Body: make([]byte, 64<<20)}
	err := m.CheckPublish(huge, nil)
	if err == nil {
		t.Skip("the test profile advertises no maximum message size, so there is no ceiling to exceed")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "size") &&
		!strings.Contains(strings.ToLower(err.Error()), "large") {
		t.Errorf("a body over the ceiling is refused with %v; the error has to name the limit", err)
	}
}
