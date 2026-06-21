package v1compat_test

import (
	"fmt"

	gps "bitbucket.org/iqhive/iqmq/drivers/gcppubsub"
	"bitbucket.org/iqhive/iqmq/drivers/gcppubsub/v1compat"
)

// Example is this arm's Open, and the thing about it a caller has to see: the
// configuration is `drivers/gcppubsub`'s Config and not a type of this
// package's own.
//
// That is the whole point of the arm. The specification is a
// bake-off between two CLIENT GENERATIONS, so the two arms have to differ by
// their client library and by nothing else — the capability table, the
// metadata mapping, the settlement state machine and the admin surface are all
// the v2 driver's and are shared. A deployment moving between them changes an
// import and nothing else.
//
// Open is not called here because it dials Google's API. The configuration is
// what an example can show; `make integration DRIVER=gcppubsub` runs the arm
// against the emulator.
func Example() {
	cfg := gps.Config{
		ProjectID: "acme-prod",
		Endpoint:  "localhost:8085", // the emulator
	}
	fmt.Printf("configured with %T\n", cfg)
	fmt.Println("client module:", v1compat.Module)
	// Output:
	// configured with gcppubsub.Config
	// client module: cloud.google.com/go/pubsub
}

// Example_deprecation is the honest half, and why the profile matters.
//
// This arm is EXPERIMENTAL and is not the default: the upstream module is
// marked deprecated in favour of cloud.google.com/go/pubsub/v2 and will stop
// receiving fixes, and when it stops building this package goes rather than
// being carried. DriverInfo.Profile reports "v1compat", so a conformance
// report or a log line made through this package can never be mistaken for one
// made through v2 — which is what keeps the bake-off's two arms distinguishable
// in the evidence afterwards.
func Example_deprecation() {
	fmt.Println("profile:", v1compat.Generation)
	// Output: profile: v1compat
}
