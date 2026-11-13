//go:build !go1.27 || !goexperiment.jsonv2

package codec

import "fmt"

// jsonv2Available reports that this build does not contain
// encoding/json/v2. The package is in the standard library from Go 1.27
// under the jsonv2 experiment, which Go 1.27 turns on by default, so a build
// with GOEXPERIMENT=nojsonv2 leaves it out; before Go 1.27 it is experimental
// (GOEXPERIMENT=jsonv2) and iqmq does not compile it. Kind.Available reports
// false for JSONv2 here and Kinds omits it.
const jsonv2Available = false

// newJSONv2 reports ErrUnavailable: this build does not contain the kind.
func newJSONv2() (Codec, error) {
	return nil, fmt.Errorf("%w: jsonv2 needs go1.27 and the jsonv2 experiment, which is on by default", ErrUnavailable)
}
