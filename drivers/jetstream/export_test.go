package jetstream

// ResolvedForTest is the defaulting Config.DSN renders from, for this
// package's tests only.
//
// A rendering writes every default out, so a configuration parsed from it
// differs from its source wherever the source left a setting at zero: "name"
// comes back "iqmq". Comparing the two RESOLVED configurations field by field
// is what says the rendering lost nothing, where comparing renderings -- which
// is all `%+v` of a Config does, because its String is DSN -- cannot fail when
// the rendering itself is what dropped the setting.
func ResolvedForTest(c Config) Config { return c.resolve() }
