//go:build !go1.27

package codec

import "fmt"

// jsonv2Available reports that this build does not contain
// encoding/json/v2: it is part of the standard library from Go 1.27, and
// experimental (GOEXPERIMENT=jsonv2) before that, so iqmq does not compile it
// on older toolchains. Kind.Available reports false for JSONv2 here and
// Kinds omits it.
const jsonv2Available = false

// newJSONv2 reports ErrUnavailable: this build does not contain the kind.
func newJSONv2() (Codec, error) { return nil, fmt.Errorf("%w: jsonv2 needs go1.27", ErrUnavailable) }
