package gcppubsub_test

import (
	"github.com/iqhive/iqmq/driverkit"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psfake"
)

// The examples read the capability table, which needs a backend, which needs a
// client. They use the scripted fake rather than a real one so that the
// examples run in `go test` with no service and no network: the specification makes the
// table constant per client, so the fake's table is the real one's.

// newExampleServer is the scripted client the examples build a backend over.
func newExampleServer(cfg gps.Config) *psfake.Server { return psfake.New(cfg.ProjectID) }

// ownedForExample is the ownership the examples pass.
func ownedForExample() driverkit.Ownership { return driverkit.ClientOwned }
