// Package jsonv2 exposes the encoding/json/v2 codec as an import path that
// compiles on every supported toolchain.
//
// encoding/json/v2 is part of the standard library from Go 1.27. The codec
// itself therefore lives in the root module's codec package behind a
// //go:build go1.27 file, and this module is the seam an application imports
// when it wants v2 explicitly and does not want a build tag of its own:
// Available reports whether the running toolchain has it, and New returns
// codec.ErrUnavailable rather than failing to compile when it does not.
//
// The module is separate from the root module for two reasons. It is the
// template every other codec module follows (proto, msgpack, avro,
// cloudevents), and it is the only place a non-standard JSON v2
// implementation could be wired in for older toolchains without adding a
// dependency to the stdlib-only core.
//
// All values returned here are stateless and safe for concurrent use.
package jsonv2

import "github.com/iqhive/iqmq/codec"

// Kind is the codec kind this module provides.
const Kind = codec.JSONv2

// Available reports whether this build contains encoding/json/v2, which is
// the case from Go 1.27 onwards.
func Available() bool { return Kind.Available() }

// New returns the encoding/json/v2 codec, or codec.ErrUnavailable when the
// toolchain that built the program predates Go 1.27.
func New() (codec.Codec, error) {
	if !Available() {
		return nil, codec.ErrUnavailable
	}
	return codec.New(Kind)
}

// Typed returns the codec bound to T, the generic shape.
func Typed[T any]() (codec.Typed[T], error) {
	c, err := New()
	if err != nil {
		return nil, err
	}
	return codec.Adapt[T](c), nil
}

// Register installs the codec in r as the implementation of
// application/json, replacing the one already registered there. It reports
// codec.ErrUnavailable and changes nothing when the build lacks v2.
func Register(r *codec.Registry) error {
	c, err := New()
	if err != nil {
		return err
	}
	return r.Replace(codec.ContentTypeJSON, c)
}
