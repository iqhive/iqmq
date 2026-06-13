package nsq_test

import (
	"errors"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/nsq"
)

// TestADSNHostCannotRedirectTheDial holds this driver's three address lists to
// refusing an entry that would send a request somewhere the configuration does
// not name.
//
// nsq was classified "parses no URL, so n/a" in the first round of, and
// that was wrong. It builds HTTP URLs out of ADDRESS LISTS rather than out of
// a DSN host: `Backend.do` concatenates "http://" + addr + path (admin.go) and
// then sets `Authorization: Bearer <AuthSecret>` on that request, and go-nsq's
// `buildLookupAddr` does the same for every LookupdAddrs entry. Measured
// against an httptest listener using that exact concatenation, all three of
// these sent "Bearer s3cret" to a host the DSN does not name:
//
//	nsq://h:4150?admin=tok%40OTHER&authsecret=s3cret
//	nsq://h:4150?lookupd=tok%40OTHER&authsecret=s3cret
//	nsq://tok%40OTHER?authsecret=s3cret      (adminAddrs derives from Addrs)
//
// and `admin=a%2Fb` / `admin=a%3Fb` each moved the request to host "a".
func TestADSNHostCannotRedirectTheDial(t *testing.T) {
	t.Parallel()
	for _, in := range []string{
		// An "@" is user information wherever net/url reads one of these, so
		// it is refused in every list.
		"nsq://tok%40real-host:4150",
		"nsq://h:4150?admin=tok%40real-host:4151",
		"nsq://h:4150?lookupd=tok%40real-host:4161",
		"nsq://u%40h:4150,v%40h:4151",
		// A host-and-port list is also ended by "/", "?" and "#".
		"nsq://a%2Fb:4150",
		"nsq://a%3Fb:4150",
		"nsq://a%23b:4150",
		"nsq://h:4150?admin=a%2Fb",
		"nsq://h:4150?admin=a%3Fb",
		"nsq://h:4150?admin=a%23b",
		// A backslash makes net/url refuse the URL at the request instead.
		"nsq://a%5Cb:4150",
		"nsq://h:4150?admin=a%5Cb",
		"nsq://h:4150?lookupd=a%5Cb",
		// The "%" rule this driver qualifies for by the same mechanism as
		// the other five: net/url rejects every "%XY" in a host but "%25",
		// so these fail at the HTTP request rather than at construction.
		"nsq://a%25b:4150",
		"nsq://tok%2540real-host:4150",
		"nsq://h:4150?admin=a%25b",
		"nsq://h:4150?lookupd=a%25b",
	} {
		_, err := nq.ParseDSN(in)
		if err == nil {
			t.Errorf("ParseDSN(%q) was accepted: the HTTP request this driver builds from that address, which carries "+
				"the auth secret, would go somewhere the DSN does not name", in)
			continue
		}
		if mq.CodeOf(err) != mq.CodeInvalidConfig || errors.Is(err, mq.ErrDriverNotRegistered) {
			t.Errorf("ParseDSN(%q) = %v (code %v), want an InvalidConfig refusal", in, err, mq.CodeOf(err))
		}
	}
	// What these lists legitimately hold still parses. A lookupd address may
	// be a full URL -- go-nsq prepends "http://" only when there is no
	// "://" and keeps a path and a query -- which is the form this
	// repository's own doc.go and example_test.go use, so a "/" in one must
	// NOT be refused.
	for _, in := range []string{
		"nsq://127.0.0.1:4150?lookupd=http://127.0.0.1:4161",
		"nsq://nsqd:4150?lookupd=http://lookupd:4161,http://lookupd2:4161",
		"nsq://nsqd:4150?lookupd=https://lookupd:4161/lookup",
		"nsq://nsqd:4150?admin=nsqd:4151,nsqd2:4151",
		"nsq://[::1]:4150",
		"nsq://[fe80::1%25eth0]:4150",
		// The zone id's "%" sits beside the literal's own colons, which is
		// the exception the rule above tests for.
		"nsq://h:4150?lookupd=http://[fe80::1%25eth0]:4161",
		"nsq://a,b:4151,c",
	} {
		if _, err := nq.ParseDSN(in); err != nil {
			t.Errorf("ParseDSN(%q) was refused: %v", in, err)
		}
	}
}
