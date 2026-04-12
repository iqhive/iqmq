package activemq

import (
	"errors"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
)

// TestParseVersion covers every shape the two brokers really send, and the
// tolerance that keeps a driver from refusing a build somebody is testing.
func TestParseVersion(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want Version
		ok   bool
	}{
		// What the brokers actually send.
		{"ActiveMQ-Artemis/2.44.0", Version{2, 44, 0}, true},
		{"ActiveMQ/6.2.0", Version{6, 2, 0}, true},
		{"2.44.0", Version{2, 44, 0}, true},
		// Tolerant of what FOLLOWS the numbers: a driver that refused a
		// snapshot build would refuse to connect to a broker under test.
		{"2.45.0-SNAPSHOT", Version{2, 45, 0}, true},
		{"6.2.0 (localhost)", Version{6, 2, 0}, true},
		{"2.44", Version{2, 44, 0}, true},
		{"6", Version{6, 0, 0}, true},
		{"  2.44.0  ", Version{2, 44, 0}, true},
		{"2.44.0.1", Version{2, 44, 0}, true},
		{"2.x", Version{2, 0, 0}, true},
		// And not tolerant of a string with no version in it at all, which
		// is what "the broker did not say" has to stay distinct from.
		{"", Version{}, false},
		{"ActiveMQ-Artemis/", Version{}, false},
		{"unknown", Version{}, false},
		{"/", Version{}, false},
		{".2.44", Version{}, false},
		// A number long enough to overflow is refused rather than wrapped: a
		// version of 99999999999 is not older than anything.
		{"99999999999.0.0", Version{}, false},
	} {
		got, ok := ParseVersion(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Errorf("ParseVersion(%q) = (%v, %v), want (%v, %v)", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

// TestVersionStringAndCompare pins the ordering and the rendering, including
// the zero value's own word.
func TestVersionStringAndCompare(t *testing.T) {
	if got := (Version{}).String(); got != "unknown" {
		t.Errorf("the zero version renders as %q, want %q: the specification makes \"the broker did not say\" distinct from any claim, and \"0.0.0\" would read as a claim", got, "unknown")
	}
	if got := (Version{2, 44, 0}).String(); got != "2.44.0" {
		t.Errorf("Version{2,44,0}.String() = %q", got)
	}
	if !(Version{}).IsZero() || (Version{Patch: 1}).IsZero() {
		t.Error("IsZero does not distinguish the zero version from 0.0.1")
	}
	for _, tc := range []struct {
		a, b Version
		want int
	}{
		{Version{2, 44, 0}, Version{2, 44, 0}, 0},
		{Version{2, 7, 0}, Version{2, 44, 0}, -1},
		{Version{2, 44, 0}, Version{2, 7, 0}, 1},
		// The minor and the patch are compared as NUMBERS, which a string
		// comparison would get wrong in exactly this place: "2.7" sorts
		// after "2.44" as text.
		{Version{2, 7, 0}, Version{2, 10, 0}, -1},
		{Version{5, 9, 0}, Version{6, 0, 0}, -1},
		{Version{2, 44, 1}, Version{2, 44, 0}, 1},
	} {
		if got := tc.a.Compare(tc.b); got != tc.want {
			t.Errorf("%v.Compare(%v) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
		if got := tc.a.AtLeast(tc.b); got != (tc.want >= 0) {
			t.Errorf("%v.AtLeast(%v) = %v", tc.a, tc.b, got)
		}
	}
}

// TestCheckAtLeast is the version gate, including the case that makes it the
// opposite of the RabbitMQ pair's rule.
//
// A broker that said NOTHING is accepted with the version left unknown,
// because Classic sends no version in its AMQP 1.0 open performative at all:
// refusing silence would refuse the broker the specification names.
func TestCheckAtLeast(t *testing.T) {
	if err := CheckAtLeast(Version{2, 7, 0}, MinArtemisFQQN, "the FQQN subscription"); err != nil {
		t.Errorf("the floor itself was refused: %v", err)
	}
	if err := CheckAtLeast(Version{2, 44, 0}, MinArtemisFQQN, "the FQQN subscription"); err != nil {
		t.Errorf("a newer broker was refused: %v", err)
	}
	if err := CheckAtLeast(Version{}, MinArtemisFQQN, "the FQQN subscription"); err != nil {
		t.Errorf("a broker that said nothing was refused: %v.\nClassic sends no version over AMQP 1.0, so refusing silence refuses one of the four combinations the driver documentation names; the product check is the half that CAN be answered from what both brokers send", err)
	}
	err := CheckAtLeast(Version{2, 6, 9}, MinArtemisFQQN, "the FQQN subscription")
	if err == nil {
		t.Fatalf("Artemis 2.6.9 satisfied the %v floor", MinArtemisFQQN)
	}
	// The refusal is Unsupported and not InvalidConfig: the broker is real
	// and reachable and does not have the feature, which is the specification's
	// Unsupported rather than a configuration error.
	if code := mq.CodeOf(err); code != mq.CodeUnsupported {
		t.Errorf("the refusal's code is %v, want %v", code, mq.CodeUnsupported)
	}
	// And it names BOTH versions and what needed one, because "too old" with
	// no numbers in it is a message somebody has to go and read code for.
	var ve *VersionError
	if !errors.As(err, &ve) {
		t.Fatalf("the refusal is not a *VersionError: %T", err)
	}
	msg := ve.Error()
	for _, want := range []string{"2.7.0", "2.6.9", "the FQQN subscription"} {
		if !contains(msg, want) {
			t.Errorf("the message %q does not name %q", msg, want)
		}
	}
	// The four floors are four numbers, which is what makes
	// "supported-version compatibility" per broker AND per protocol.
	floors := map[string]Version{
		"artemis":       MinArtemis,
		"artemis-fqqn":  MinArtemisFQQN,
		"classic-amqp":  MinClassicAMQP,
		"classic-stomp": MinClassicSTOMP,
	}
	for name, v := range floors {
		if v.IsZero() {
			t.Errorf("the %s floor is the zero version, so nothing would ever be refused against it", name)
		}
	}
	if !MinArtemisFQQN.AtLeast(MinArtemis) {
		t.Errorf("the FQQN floor %v is older than the address-model floor %v; FQQN arrived later", MinArtemisFQQN, MinArtemis)
	}
	if !MinClassicAMQP.AtLeast(MinClassicSTOMP) {
		t.Errorf("the Classic AMQP floor %v is older than the Classic STOMP floor %v; STOMP 1.2 arrived in 5.6 and the AMQP transport stopped being experimental in 5.9", MinClassicAMQP, MinClassicSTOMP)
	}
}

// TestCheckProductBothDirections is the gate the version check cannot be, and
// it is checked in BOTH directions because a prefix match gets one of them
// wrong.
//
// Artemis's STOMP `server` header is "ActiveMQ-Artemis/2.44.0" and Classic's
// is "ActiveMQ/6.2.0", so a Classic test written as `HasPrefix(s,
// "ActiveMQ")` matches Artemis as well — and the failure that lets through is
// exactly the one this function exists to stop: a client configured for
// Classic and pointed at Artemis does not fail, it AUTO-CREATES an address
// called `topic://VirtualTopic.orders`, publishes into it for ever, and its
// consumers attach to `queue://Consumer.workers.VirtualTopic.orders`, which is
// a different address again. Nothing reports an error and nothing is
// delivered.
func TestCheckProductBothDirections(t *testing.T) {
	for _, tc := range []struct {
		name    string
		want    Broker
		product string
		ok      bool
	}{
		// The right broker, in every spelling each one sends.
		{"artemis amqp product", Artemis, ProductArtemis, true},
		{"artemis stomp server", Artemis, "ActiveMQ-Artemis/2.44.0", true},
		{"artemis stomp server bare", Artemis, ServerArtemis, true},
		{"classic stomp server", Classic, "ActiveMQ/6.2.0", true},
		{"classic stomp server bare", Classic, ServerClassic, true},
		// Silence, which is the ORDINARY case for Classic over AMQP 1.0.
		{"artemis silence", Artemis, "", true},
		{"classic silence", Classic, "", true},
		// Silence is decided after trimming, because productBroker trims
		// too: a `server` header of one space was neither silence nor
		// placeable, so it refused the whole connection. FuzzActiveMQVersion
		// found that, and this is what keeps it fixed.
		{"artemis whitespace is silence", Artemis, " ", true},
		{"classic whitespace is silence", Classic, "\t\n ", true},
		// The wrong broker, in both directions. The second row is the one a
		// prefix match gets wrong.
		{"classic configured, artemis answered (stomp)", Classic, "ActiveMQ-Artemis/2.44.0", false},
		{"classic configured, artemis answered (amqp)", Classic, ProductArtemis, false},
		{"artemis configured, classic answered", Artemis, "ActiveMQ/6.2.0", false},
		// And a product neither of them is.
		{"artemis configured, rabbit answered", Artemis, "rabbitmq", false},
		{"classic configured, rabbit answered", Classic, "RabbitMQ", false},
		{"artemis configured, nonsense answered", Artemis, "ActiveMQish/1.0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckProduct(tc.want, tc.product)
			if tc.ok {
				if err != nil {
					t.Fatalf("CheckProduct(%v, %q) = %v, want nil", tc.want, tc.product, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("CheckProduct(%v, %q) accepted the wrong product: the two products' topic mappings name nothing in common, and Artemis would auto-create the wrong one rather than refuse", tc.want, tc.product)
			}
			if code := mq.CodeOf(err); code != mq.CodeUnsupported {
				t.Errorf("the refusal's code is %v, want %v", code, mq.CodeUnsupported)
			}
			var pe *ProductError
			if !errors.As(err, &pe) {
				t.Fatalf("the refusal is not a *ProductError: %T", err)
			}
			if pe.Want != tc.want || pe.Product != tc.product {
				t.Errorf("the refusal records (want=%v, product=%q), want (%v, %q)", pe.Want, pe.Product, tc.want, tc.product)
			}
			if !contains(pe.Error(), tc.want.String()) {
				t.Errorf("the message %q does not name the configured broker %q", pe.Error(), tc.want)
			}
		})
	}
	// The case above spelled out as the property, so that a future rewrite of
	// productBroker in terms of prefixes fails here: "ActiveMQ-Artemis/..."
	// starts with "ActiveMQ", and reading it as Classic is silent message
	// loss rather than a refusal.
	if err := CheckProduct(Classic, ServerArtemis+"/2.44.0"); err == nil {
		t.Fatalf("a Classic configuration accepted %q: it starts with %q, so a prefix match reads Artemis as Classic, which is the one mistake CheckProduct exists to stop",
			ServerArtemis+"/2.44.0", ServerClassic)
	}
	// Case is folded, because neither header's case is promised.
	if err := CheckProduct(Artemis, "apache-ACTIVEMQ-artemis"); err != nil {
		t.Errorf("the product check is case-sensitive: %v", err)
	}
	if err := CheckProduct(Classic, "activemq/6.2.0"); err != nil {
		t.Errorf("the Classic server header's case is not folded: %v", err)
	}
}

// contains is strings.Contains, local so that the failure messages above read
// as assertions rather than as string plumbing.
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
