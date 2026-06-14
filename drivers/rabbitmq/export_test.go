package rabbitmq

// ResolvedForTest is the defaulting a rendering writes out, for this package's
// tests only.
//
// Config.DSN renders every default, so a configuration parsed back from a
// rendering differs from its source wherever the source left a setting at
// zero. Comparing the two RESOLVED configurations field by field is what says
// the rendering lost nothing, where comparing renderings cannot fail when the
// rendering itself is what dropped the setting.
func ResolvedForTest(c Config) Config { return c.resolve() }

// URLsForTest is the dial URL list Config.urls builds, for this package's
// tests only. It is what reconnect.go hands amqp.DialConfig, so a test that
// reads it is reading what is really dialled.
func URLsForTest(c Config) []string { return c.resolve().urls() }
