package amqp10

import (
	"testing"

	core "bitbucket.org/iqhive/iqmq/drivers/amqp10"

	"bitbucket.org/iqhive/iqmq/testkit"
)

// TestClientPinMatchesTheLinkedModule checks that the advertised client
// version matches the module linked into this test binary.
//
// [testkit.CheckClientPin] uses runtime build information so the
// comparison includes module-graph version selection.
func TestClientPinMatchesTheLinkedModule(t *testing.T) {
	testkit.CheckClientPin(t, testkit.ClientInfo{Module: core.ClientModule, Version: core.ClientVersion})
}
