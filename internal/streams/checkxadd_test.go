package streams_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/internal/streams"
)

// The property under test here is an EQUIVALENCE, not a behaviour.
//
// [streams.CheckXAdd] exists only because the publish paths discard XADD's
// identifier, and it is safe only for as long as it answers exactly what
// [streams.ParseXAdd] answers. The dangerous direction is one-sided: a reply
// ParseXAdd refuses and CheckXAdd accepts is a corrupt or refused publication
// reported to the caller as a success, which the specification forbids and which nothing
// downstream can detect. So every assertion below compares the two functions
// against each other on the same input, and the identifier-level pair
// [streams.ValidID] and [streams.ParseID] the same way. A test that asserted
// CheckXAdd's answers from a table would go on passing after somebody widened
// ParseID.

// xaddReplyCases are the reply classes the two functions have to agree on,
// each named by what it is rather than by what it returns -- the expected
// error is ParseXAdd's own answer, read at run time.
var xaddReplyCases = []struct {
	name string
	v    streams.Value
}{
	{"NOMKSTREAM's null refusal", streams.Nil()},
	{"an integer reply", streams.Int(3)},
	{"an array reply", streams.Arr(streams.Str("1-1"))},
	{"a boolean reply", streams.Bool(true)},
	{"the empty string", streams.Str("")},
	{"a plain identifier", streams.Str("1767225600123-0")},
	{"a bare millisecond", streams.Str("1767225600123")},
	{"a high sequence", streams.Str("1767225600123-18446744073709551615")},
	{"nonsense", streams.Str("nonsense")},
	{"a trailing separator", streams.Str("17-")},
	{"a leading separator", streams.Str("-17")},
	{"two separators", streams.Str("1-2-3")},
	{"a signed millisecond", streams.Str("+17-0")},
	{"a millisecond that overflows uint64", streams.Str("18446744073709551616-0")},
	{"a sequence that overflows uint64", streams.Str("17-18446744073709551616")},
	{"leading zeros", streams.Str("0000000000000000000000017-0")},
	{"an underscore separator Go accepts at base 0", streams.Str("1_7-0")},
	{"a hex-looking millisecond", streams.Str("0x11-0")},
	{"an identifier with a NUL in it", streams.Str("17\x00-0")},
}

func TestCheckXAddAnswersExactlyWhatParseXAddDoes(t *testing.T) {
	t.Parallel()
	for _, tc := range xaddReplyCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, want := streams.ParseXAdd(tc.v)
			got := streams.CheckXAdd(tc.v)
			if got != want { //nolint:errorlint // identity is the assertion: errcode matches by Code, so errors.Is cannot tell ErrReplyShape from ErrBadID
				t.Errorf("CheckXAdd(%v) = %v, ParseXAdd = %v; the two must be the same answer", tc.v, got, want)
			}
		})
	}
}

// TestCheckXAddStillDetectsNoStream is the one answer that is named rather than
// compared, because it is the answer a FEATURE depends on: NOMKSTREAM refusing
// to create a stream arrives as a null reply and must reach the caller as
// [streams.ErrNoStream] (NotFound), not as corruption and not as a success. The
// equivalence test above would still pass if both functions returned nil here.
func TestCheckXAddStillDetectsNoStream(t *testing.T) {
	t.Parallel()
	if err := streams.CheckXAdd(streams.Nil()); err != streams.ErrNoStream { //nolint:errorlint // identity is the assertion
		t.Errorf("CheckXAdd(nil reply) = %v, want ErrNoStream: NOMKSTREAM's refusal is detected by the null and nothing else", err)
	}
	if err := streams.CheckXAdd(streams.Str("1767225600123-0")); err != nil {
		t.Errorf("CheckXAdd(an identifier) = %v, want nil", err)
	}
}

// idStrings are the identifier spellings the validator and the parser have to
// agree on, including the three boundaries a digits-only validator gets wrong:
// the uint64 maximum, one past it, and a value whose significant digits are
// hidden behind leading zeros.
var idStrings = []string{
	"", "0", "0-0", "1", "17-4", "17-", "-17", "-", "1-2-3", "+1-0", "1-+0",
	"1767225600123-0", "1767225600123-99", "nonsense", "1x-0", "0x11",
	"18446744073709551615", "18446744073709551616",
	"18446744073709551615-18446744073709551615",
	"18446744073709551615-18446744073709551616",
	"9999999999999999999", "99999999999999999999", "999999999999999999999",
	"00000000000000000000000000017", "0000000000000000000018446744073709551615",
	"0000000000000000000018446744073709551616",
	"1_7", "1-1_7", " 17", "17 ", "17\x00", "\x0017",
	strings.Repeat("9", 64), strings.Repeat("0", 64), strings.Repeat("0", 64) + "1",
}

func TestValidIDAgreesWithParseID(t *testing.T) {
	t.Parallel()
	for _, s := range idStrings {
		_, err := streams.ParseID(s)
		if got := streams.ValidID(s); got != (err == nil) {
			t.Errorf("ValidID(%q) = %v, ParseID error = %v; the two must agree", s, got, err)
		}
	}
}

// FuzzCheckXAddMatchesParseXAdd is the equivalence over inputs nobody wrote
// down. Each input string is offered as every reply shape that can carry a
// string and as the shapes that cannot, so the fuzzer explores the identifier
// grammar and the shape branch in one target.
func FuzzCheckXAddMatchesParseXAdd(f *testing.F) {
	for _, s := range idStrings {
		f.Add(s)
	}
	f.Add("1767225600123-18446744073709551615")
	f.Fuzz(func(t *testing.T, s string) {
		for _, v := range []streams.Value{
			streams.Str(s),
			streams.Nil(),
			streams.Int(int64(len(s))),
			streams.Arr(streams.Str(s)),
		} {
			_, want := streams.ParseXAdd(v)
			if got := streams.CheckXAdd(v); got != want { //nolint:errorlint // identity is the assertion
				t.Fatalf("CheckXAdd(%v of %q) = %v, ParseXAdd = %v", v.Kind(), s, got, want)
			}
		}
		// The parser is the reference for the validator too: a validator that
		// accepted what ParseID refuses would make CheckXAdd's nil a lie about
		// a reply the rest of the driver would refuse.
		_, perr := streams.ParseID(s)
		if streams.ValidID(s) != (perr == nil) {
			t.Fatalf("ValidID(%q) = %v, ParseID error = %v", s, streams.ValidID(s), perr)
		}
	})
}

// FuzzValidIDOnDecimalStrings narrows the fuzzer onto the part a string fuzzer
// almost never reaches on its own: long runs of digits around the uint64
// boundary. An arbitrary []byte is overwhelmingly not a decimal number, so the
// corpus above would exercise the "not a digit" branch for ever and the
// overflow comparison hardly at all. Here the input BUILDS a digit string --
// a count of leading zeros, then digits derived from the bytes -- so every
// execution lands inside validUint64's arithmetic.
func FuzzValidIDOnDecimalStrings(f *testing.F) {
	f.Add(uint8(0), uint64(0), uint64(0))
	f.Add(uint8(3), uint64(17), uint64(4))
	f.Add(uint8(0), ^uint64(0), ^uint64(0))
	f.Add(uint8(21), ^uint64(0), uint64(0))
	f.Fuzz(func(t *testing.T, zeros uint8, ms, seq uint64) {
		pad := strings.Repeat("0", int(zeros%40))
		for _, s := range []string{
			pad + strconv.FormatUint(ms, 10),
			pad + strconv.FormatUint(ms, 10) + "-" + strconv.FormatUint(seq, 10),
			pad + strconv.FormatUint(ms, 10) + "0", // one digit wider than ms
			pad + strconv.FormatUint(ms, 10) + "-" + pad + strconv.FormatUint(seq, 10) + "0",
		} {
			_, perr := streams.ParseID(s)
			if got := streams.ValidID(s); got != (perr == nil) {
				t.Fatalf("ValidID(%q) = %v, ParseID error = %v", s, got, perr)
			}
			if got, want := streams.CheckXAdd(streams.Str(s)), parseXAddErr(streams.Str(s)); got != want { //nolint:errorlint // identity is the assertion
				t.Fatalf("CheckXAdd(%q) = %v, ParseXAdd = %v", s, got, want)
			}
		}
	})
}

func parseXAddErr(v streams.Value) error {
	_, err := streams.ParseXAdd(v)
	return err
}
