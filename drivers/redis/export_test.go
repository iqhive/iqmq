package redis

// ResolvedForTest is the defaulting [Config.DSN] renders from, for this
// package's tests only.
//
// A rendering writes every default out, so a configuration parsed back from
// one differs from its source wherever the source left a setting at zero.
// Comparing the two RESOLVED configurations field by field is what says the
// rendering lost nothing, where comparing renderings cannot fail when the
// rendering itself is what dropped the setting.
//
// Nack is resolved here and not in `resolve`, because this driver resolves it
// at each point of use -- the capability table, the source and the rendering
// all call `Nack.Resolve()` -- so a Config holding Auto and one holding the
// kind Auto resolves to are the same configuration. Comparing them without
// this made every row of the render test fail for a reason that was the
// instrument's rather than the renderer's.
func ResolvedForTest(c Config) Config {
	c = c.resolve()
	c.Nack = c.Nack.Resolve()
	return c
}
