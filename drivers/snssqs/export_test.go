package snssqs

// ResolvedForTest is the defaulting [Config.DSN] renders from, for this
// package's tests only.
//
// A rendering writes every default out, so a configuration parsed back from
// one differs from its source wherever the source left a setting at zero.
// Comparing the two RESOLVED configurations field by field is what says the
// rendering lost nothing, where comparing renderings cannot fail when the
// rendering itself is what dropped the setting.
func ResolvedForTest(c Config) Config { return c.resolve() }

// EscapePathForTest exposes the path escaper, for this package's tests only.
//
// This is the THIRD identical copy of an escaper the validator makes
// unreachable, and the specification pinned two of them -- sqs's and
// pulsar's -- while this one, the byte-identical twin edited in the same
// commit, stayed unwatched. A rule applied to some of its instances is a rule
// that has not been applied.
func EscapePathForTest(s string) string { return escapePath(s) }
