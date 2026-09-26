package goredis_test

import (
	"reflect"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/drivers/redis/goredis"
	"github.com/iqhive/iqmq/dsn"
)

// TestParseDSNIsTheSameAnswerAsTheRueidisDriversreads is the configuration
// half of "one broker, one DSN".
//
// `drivers/redis` documents the decision this test holds: a DSN naming "redis"
// has to mean one thing, and which client library serves it is a build-time
// choice a connection string has no business making. So this module registers
// no scheme, and its ParseDSN is the other driver's — accepting the same
// strings, refusing the same strings, and producing the same Config. A copy of
// the parameter table would be a second place for `nack=zsetmover` to mean
// something slightly different.
func TestParseDSNIsTheSameAnswerAsTheRueidisDrivers(t *testing.T) {
	accepted := []string{
		"redis://127.0.0.1:6379",
		"redis://user:pw@10.0.0.1:6379,10.0.0.2:6379/3?partitions=4&nack=zsetmover",
		"redis://h/0?ackdeadline=45s&claiminterval=5s&claimcount=32&block=1s",
		"redis://h?autocreate=true&maxlen=1000&keyprefix=iq.",
		"redis://h?tls=true&insecureskipverify=true",
		"redis://h?deadlettersuffix=:dead&ackguard=owner",
	}
	for _, s := range accepted {
		t.Run(short(s), func(t *testing.T) {
			want, werr := rd.ParseDSN(s)
			got, gerr := goredis.ParseDSN(s)
			if (werr == nil) != (gerr == nil) {
				t.Fatalf("rueidis says %v and goredis says %v", werr, gerr)
			}
			if werr != nil {
				return
			}
			if got.DSN() != want.DSN() {
				t.Errorf("the two drivers parsed one DSN differently:\n goredis %s\n rueidis %s",
					got.DSN(), want.DSN())
			}
			if got.Password.Reveal() != want.Password.Reveal() {
				t.Error("the credential did not survive one of the two parses")
			}
		})
	}
	refused := []string{
		"goredis://h",
		"redis://h?nack=auto",
		"redis://h?nack=nosuchkind",
		"redis://h?partitions=-1",
		"redis://h?insecureskipverify=true",
		"redis://h?nosuchparameter=1",
		"redis://h/notanumber",
	}
	for _, s := range refused {
		t.Run("refused/"+short(s), func(t *testing.T) {
			if _, err := goredis.ParseDSN(s); err == nil {
				t.Fatalf("ParseDSN accepted %q", s)
			}
			if _, err := rd.ParseDSN(s); err == nil {
				t.Fatalf("the rueidis driver accepted %q, so the two disagree", s)
			}
		})
	}
}

// TestTheSchemeBelongsToTheBrokerAndNotToTheClient is the negative half: a
// registry this module cannot register itself into still resolves "redis", and
// resolves it to the rueidis driver, because that is the one the scheme names.
func TestTheSchemeBelongsToTheBrokerAndNotToTheClient(t *testing.T) {
	r := &dsn.Registry{}
	if err := rd.Register(r); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, ok := r.Lookup(rd.Scheme); !ok {
		t.Fatalf("the registry does not resolve %q after the rueidis driver registered it", rd.Scheme)
	}
	if _, ok := r.Lookup("goredis"); ok {
		t.Error(`the registry resolves "goredis": a client library is not a scheme`)
	}
}

// TestTheConfigTypeIsShared holds the alias, and holds it as an IDENTITY
// rather than as an assignment.
//
// An assignment between the two would compile whether the alias is an alias or
// a defined type with the same fields, so it is a check that cannot fail —
// which is what the linter said about the first version of this test. Type
// identity is the property that matters: a defined type of its own would make
// every configuration helper, every option and every DSN round trip need a
// conversion, and a deployment could not change client library without
// rewriting its configuration.
func TestTheConfigTypeIsShared(t *testing.T) {
	if got, want := reflect.TypeFor[goredis.Config](), reflect.TypeFor[rd.Config](); got != want {
		t.Fatalf("goredis.Config is %v and redis.Config is %v; the alias has become a type of its own",
			got, want)
	}
	cfg := goredis.Config{Addrs: []string{"127.0.0.1:6379"}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

// TestUniversalOptionsDisablesGoRedisOwnRetries is.
//
// go-redis retries a failed command by default, including one it has already
// written, and on this broker a retried XADD is a second copy of the message
// presented to the caller as one successful publication — the specification's
// duplicate-reported-as-success and the disqualifier. Rueidis does not
// retry a written command, so leaving go-redis's default on would also make
// the two clients differ in whether a publication can silently duplicate.
func TestUniversalOptionsDisablesGoRedisOwnRetries(t *testing.T) {
	opt := goredis.UniversalOptions(rd.Config{Addrs: []string{"h:6379"}})
	if opt.MaxRetries != -1 {
		t.Errorf("MaxRetries = %d, want -1 (go-redis's spelling of \"do not retry\"): a retry of a "+
			"written XADD is a duplicate the caller is told is one publication", opt.MaxRetries)
	}
	if opt.DialTimeout != rd.DefaultDialTimeout {
		t.Errorf("DialTimeout = %v, want the driver's default %v", opt.DialTimeout, rd.DefaultDialTimeout)
	}
	if len(opt.Addrs) != 1 || opt.Addrs[0] != "h:6379" {
		t.Errorf("Addrs = %v", opt.Addrs)
	}
	// An empty address list becomes the driver's own default rather than
	// go-redis's, so that both clients dial the same place.
	if got := goredis.UniversalOptions(rd.Config{}); len(got.Addrs) != 1 || got.Addrs[0] != rd.DefaultAddr {
		t.Errorf("an empty Config dials %v, want %q", got.Addrs, rd.DefaultAddr)
	}
}

func short(s string) string {
	s = strings.TrimPrefix(s, "redis://")
	if i := strings.IndexByte(s, '?'); i >= 0 {
		s = s[i+1:]
	}
	if len(s) > 28 {
		s = s[:28]
	}
	return s
}

var _ = mq.ErrUnsupported
