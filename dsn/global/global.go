// Package global is the process-wide DSN registry, for a program that wants one
// (the API, ADR-012).
//
// It is a module of its own, and that is the whole design. The root module has
// no global mutable state — the specification makes that a property of the package, and it
// is what lets two clients of one driver, or two libraries with different
// driver sets, coexist in one process. A package-level registry in the root
// module would take that away from every program that imports iqmq, whether or
// not it wanted one. Here, a program opts in by importing this module, and a
// library that imports it takes the same global away from its own callers,
// which is a decision an import graph makes visible.
//
// The convenience it buys is the blank-import idiom:
//
//	import (
//		"github.com/iqhive/iqmq/dsn/global"
//		_ "github.com/iqhive/iqmq/drivers/kafka"
//	)
//
//	client, err := global.Open(ctx, os.Getenv("BROKER_DSN"))
//
// A driver never imports this module. A driver exposes Register(dsn.Registrar)
// and nothing else, and the driver package a program blank-imports is
// what puts the two together.
package global

import (
	"context"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
)

// registry is the process-wide registry. It is package state on purpose: that
// is what this module is.
var registry = dsn.NewRegistry()

// Registry returns the process-wide registry, for a caller that needs the whole
// surface — RegisterDriver, Schemes, DriverName.
func Registry() *dsn.Registry { return registry }

// Register associates a scheme with the function that opens a client from a
// DSN. It fails when the scheme is already taken.
func Register(scheme string, open dsn.OpenFunc) error {
	return registry.Register(scheme, open)
}

// RegisterDriver registers every scheme a driver answers to.
func RegisterDriver(d dsn.Driver) error { return registry.RegisterDriver(d) }

// MustRegister is [Register] for a package initialiser, where there is nobody
// to return an error to. It panics on a conflict, which is the honest answer:
// two drivers claiming one scheme is a build that cannot work, and discovering
// it at the first Open instead of at start-up is strictly worse.
func MustRegister(scheme string, open dsn.OpenFunc) {
	if err := Register(scheme, open); err != nil {
		panic(err)
	}
}

// MustRegisterDriver is [RegisterDriver] for a package initialiser.
func MustRegisterDriver(d dsn.Driver) {
	if err := RegisterDriver(d); err != nil {
		panic(err)
	}
}

// Open opens a client from a DSN through the process-wide registry. A
// scheme nothing registered reports
// [github.com/iqhive/iqmq.ErrDriverNotRegistered].
func Open(ctx context.Context, s string) (*mq.Client, error) { return registry.Open(ctx, s) }

// Schemes returns every registered scheme, sorted.
func Schemes() []string { return registry.Schemes() }
