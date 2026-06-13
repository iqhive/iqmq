package pulsar

// ResolvedForTest is the defaulting a rendering writes out, for this package's
// tests only.
//
// A rendering writes the defaults it carries, so a configuration parsed back
// from one differs from its source wherever the source left such a setting at
// zero. Comparing the two RESOLVED configurations field by field is what says
// the rendering lost nothing, where comparing renderings cannot fail when the
// rendering itself is what dropped the setting.
func ResolvedForTest(c Config) Config { return c.resolve() }

// EscapePathForTest exposes the path escaper, for this package's tests only.
//
// It is exported because the escaper is currently UNREACHABLE through the
// parser: a tenant, namespace or topic prefix is validated to alphanumerics and a
// little punctuation, so no input the validator admits carries a byte the
// path position decodes, and reverting the escaper failed no test. That makes
// it a guard rather than dead code -- it is what keeps the rendering correct
// if the validator is ever loosened -- and a guard nobody has watched fail is
// assumed not to work, so it is tested directly instead.
func EscapePathForTest(s string) string { return escapePath(s) }
