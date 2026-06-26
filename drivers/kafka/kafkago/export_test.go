package kafkago

// ResolvedForTest is the defaulting [Config.DSN] renders from, for this
// package's tests only.
//
// A rendering writes every default out, so a configuration parsed back from
// one differs from its source wherever the source left a setting at zero.
// Comparing the two RESOLVED configurations field by field is what says the
// rendering lost nothing, where comparing renderings cannot fail when the
// rendering itself is what dropped the setting.
func ResolvedForTest(c Config) Config { return c.resolve() }
