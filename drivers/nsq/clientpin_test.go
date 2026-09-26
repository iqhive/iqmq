package nsq

import (
	"testing"

	"github.com/iqhive/iqmq/testkit"
)

// TestClientPinMatchesTheLinkedModule checks that the advertised client
// version matches the module linked into this test binary.
//
// [testkit.CheckClientPin] uses runtime build information so the
// comparison includes module-graph version selection.
func TestClientPinMatchesTheLinkedModule(t *testing.T) {
	testkit.CheckClientPin(t, testkit.ClientInfo{Module: ClientModule, Version: ClientVersion})
}
