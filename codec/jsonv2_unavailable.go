//go:build !go1.27

package codec

// jsonv2Available reports that this build does not contain
// encoding/json/v2: it is part of the standard library from Go 1.27, and
// experimental (GOEXPERIMENT=jsonv2) before that, so iqmq does not compile it
// on older toolchains. Kind.Available reports false for JSONv2 here and
// Kinds omits it.
const jsonv2Available = false

// newJSONv2 panics: New documents that it panics on a kind this build does
// not contain, and Available exists so callers never reach this.
func newJSONv2() Codec { panic(ErrUnavailable.Error() + ": jsonv2 needs go1.27") }
