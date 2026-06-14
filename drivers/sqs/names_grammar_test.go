package sqs_test

import (
	"strings"
	"testing"

	sq "bitbucket.org/iqhive/iqmq/drivers/sqs"
)

// The name grammar, which is pure and runs before any I/O.
//
// That is what makes a topic the mapping cannot express an immediate refusal
// naming the rule, rather than a signed request the service rejects with a
// message about a parameter. Each of these is a rule the service has and a
// caller can trip.

// TestCheckQueueNameHoldsEveryRuleTheServiceHas.
//
// The FIFO suffix is in both directions on purpose: a queue name ending
// `.fifo` on a standard profile is as wrong as one without it on a FIFO
// profile, and a checker that only looked one way would let half of the
// mismatches through.
func TestCheckQueueNameHoldsEveryRuleTheServiceHas(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arg  string
		fifo bool
		ok   bool
		// want is a phrase the refusal has to carry. A bare "it failed" is
		// not enough for the two FIFO cases: `orders.fifo` on a standard
		// queue is refused by the CHARACTER rule as well, since a dot is not
		// in the alphabet, so a test that only asked whether it failed would
		// pass with the FIFO rule deleted.
		want string
	}{
		{"an ordinary name", "orders", false, true, ""},
		{"a name with a dash and an underscore", "app-orders_v2", false, true, ""},
		{"the empty name", "", false, false, "empty"},
		{"a name over the service's length", strings.Repeat("x", sq.MaxQueueNameLen+1), false, false, "eighty characters"},
		{"a name with a dot outside the suffix", "orders.v2", false, false, "alphanumerics"},
		{"a name with a space", "my orders", false, false, "alphanumerics"},
		{"a name with a slash", "app/orders", false, false, "alphanumerics"},
		{"a FIFO name on a standard queue", "orders.fifo", false, false, "needs the FIFO profile"},
		{"a FIFO name on a FIFO queue", "orders.fifo", true, true, ""},
		{"a standard name on a FIFO queue", "orders", true, false, "ending in .fifo"},
		{"the suffix and nothing else", ".fifo", true, false, "empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := sq.CheckQueueName(tc.arg, tc.fifo)
			if tc.ok {
				if err != nil {
					t.Errorf("CheckQueueName(%q, fifo=%t) is %v, want nil", tc.arg, tc.fifo, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("CheckQueueName(%q, fifo=%t) accepted it", tc.arg, tc.fifo)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("CheckQueueName(%q, fifo=%t) is %v, want a refusal mentioning %q",
					tc.arg, tc.fifo, err, tc.want)
			}
		})
	}
}

// TestAGroupIdentifierIsTheKeyOrAHashOfIt.
//
// A key the service will accept is passed through, because it is what a
// console shows and what an operator correlates with. One it will not -- too
// long, or carrying anything outside printable ASCII -- is hashed, which
// keeps "two messages with the same key are delivered in order" true while
// costing only FIFO throughput on a collision.
func TestAGroupIdentifierIsTheKeyOrAHashOfIt(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		key    string
		hashed bool
	}{
		{"a short printable key", "customer-42", false},
		{"an empty key", "", false}, // the default group, not a hash
		{"a key with a space", "customer 42", true},
		{"a key with a newline", "customer\n42", true},
		{"a key with a non-ASCII rune", "customer-café", true},
		{"a key over the length", strings.Repeat("k", 129), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := sq.GroupID([]byte(tc.key))
			switch {
			case tc.key == "":
				if got != sq.DefaultGroupID {
					t.Errorf("GroupID of an empty key is %q, want the default group: a FIFO queue REQUIRES "+
						"a group, and inventing a unique one per message would turn the profile a caller "+
						"chose for its ordering into an unordered one", got)
				}
			case tc.hashed:
				if !strings.HasPrefix(got, "h-") {
					t.Errorf("GroupID(%q) is %q; a key the service will not accept has to be hashed", tc.key, got)
				}
			default:
				if got != tc.key {
					t.Errorf("GroupID(%q) is %q; a key the service accepts is passed through, because it is "+
						"what the console shows and what an operator correlates with", tc.key, got)
				}
			}
		})
	}
}

// TestADeduplicationIdentifierUsesTheSameGrammarExceptForTheEmptyKey.
//
// The difference is the whole of it: an empty GROUP has to become the default
// group because a FIFO queue requires one, and an empty DEDUP key means the
// caller asked for no deduplication at all -- so rendering one as the other
// would deduplicate every keyless publication against itself.
func TestADeduplicationIdentifierUsesTheSameGrammarExceptForTheEmptyKey(t *testing.T) {
	t.Parallel()

	if got := sq.DedupID(""); got != "" {
		t.Errorf("DedupID of an empty key is %q, want empty: a caller who set no key asked for no "+
			"deduplication, and a default would deduplicate every keyless publication against itself", got)
	}
	if got := sq.DedupID("order-42"); got != "order-42" {
		t.Errorf("DedupID of an acceptable key is %q, want it passed through", got)
	}
	long := strings.Repeat("d", 129)
	if got := sq.DedupID(long); !strings.HasPrefix(got, "h-") {
		t.Errorf("DedupID of an over-long key is %q, want a hash", got)
	}
	// The same key renders the same way every time, or a retry would be a
	// different publication and the deduplication would never fire. The two
	// calls are kept in variables because staticcheck reads
	// `f(x) != f(x)` as a mistake, and here it is the assertion.
	first, second := sq.DedupID(long), sq.DedupID(long)
	if first != second {
		t.Errorf("DedupID rendered the same key as %q and then %q; a retry would be a different "+
			"publication and the deduplication window would never match", first, second)
	}
}

// TestAQueueNameIsRecoveredFromEveryShapeOfURL.
//
// The service has handed out two shapes over the years -- a path and an ARN
// -- and a driver that understood one would fail to name the queue in half
// the errors it reports.
func TestAQueueNameIsRecoveredFromEveryShapeOfURL(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ url, want string }{
		{"https://sqs.us-east-1.amazonaws.com/000000000000/orders", "orders"},
		{"arn:aws:sqs:us-east-1:000000000000:orders", "orders"},
		{"orders", "orders"},
	} {
		if got := sq.QueueNameOfURL(tc.url); got != tc.want {
			t.Errorf("QueueNameOfURL(%q) is %q, want %q", tc.url, got, tc.want)
		}
	}
}

// TestTheTopicOfAQueueNameIsTheInverseOfTheMapping.
//
// It is what ListTopics owes its caller: a name they can pass back. The
// prefix refusal is the interesting half -- a queue that does not carry the
// configured prefix belongs to another deployment, and reporting it as a
// topic would invite somebody to delete it.
func TestTheTopicOfAQueueNameIsTheInverseOfTheMapping(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, queue, prefix, want string
		ok                        bool
	}{
		{"no prefix configured", "orders", "", "orders", true},
		{"the prefix is removed", "app-orders", "app-", "orders", true},
		{"a FIFO suffix is removed", "app-orders.fifo", "app-", "orders", true},
		{"another deployment's queue", "other-orders", "app-", "", false},
		{"the prefix and nothing else", "app-", "app-", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := sq.TopicOfQueueName(tc.queue, tc.prefix)
			if ok != tc.ok {
				t.Fatalf("TopicOfQueueName(%q, %q) reported ok=%t, want %t", tc.queue, tc.prefix, ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Errorf("TopicOfQueueName(%q, %q) is %q, want %q", tc.queue, tc.prefix, got, tc.want)
			}
		})
	}
}

// TestEveryWildcardMetacharacterIsRecognised.
//
// The set is the union of the ones the brokers iqmq speaks to use, because
// what the function answers is "did the caller mean a pattern?" -- and a
// caller who meant one on any other broker means one here.
func TestEveryWildcardMetacharacterIsRecognised(t *testing.T) {
	t.Parallel()
	for _, s := range []string{"orders.*", "orders.?", "orders.#", "orders.>"} {
		if !sq.HasWildcard(s) {
			t.Errorf("HasWildcard(%q) is false; a caller who meant a pattern on another broker means one here", s)
		}
	}
	for _, s := range []string{"orders", "app-orders_v2", ""} {
		if sq.HasWildcard(s) {
			t.Errorf("HasWildcard(%q) is true", s)
		}
	}
}
