package driverkit

import (
	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Ownership says whether iqmq closes the native client it was given (the specification's
// "support OpenWithClient with explicit Borrowed/Owned").
//
// Every official driver offers OpenWithClient, and the question it raises is who
// closes the client. Making the caller say is the only answer that is right
// twice: a driver that always closed would tear down a connection pool the
// application still uses, and one that never closed would leak the client it
// dialled itself. There is no default, because the zero value has to mean
// something and "borrowed" is the safe half — iqmq closing nothing it was not
// asked to close.
type Ownership uint8

// The ownership modes.
const (
	// ClientBorrowed means the caller owns the native client and closes it.
	// It is the zero value: iqmq never closes something it was handed
	// without being asked to.
	ClientBorrowed Ownership = iota
	// ClientOwned means iqmq owns the native client and closes it with the
	// backend.
	ClientOwned

	ownershipCount
)

var ownershipNames = [ownershipCount]string{"borrowed", "owned"}

// String returns the mode's lower-case name.
func (o Ownership) String() string {
	if o < ownershipCount {
		return ownershipNames[o]
	}
	return "Ownership(?)"
}

// Valid reports whether o names an ownership mode.
func (o Ownership) Valid() bool { return o < ownershipCount }

// Owned reports whether iqmq closes the native client.
func (o Ownership) Owned() bool { return o == ClientOwned }

// Close calls closeNative when iqmq owns the client, and does nothing when it
// does not. A nil closer is a driver with nothing to close.
func (o Ownership) Close(closeNative func() error) error {
	if !o.Owned() || closeNative == nil {
		return nil
	}
	return closeNative()
}

// CheckOwnership refuses an ownership value outside the enumeration, which a
// driver's OpenWithClient does before it starts any background work (the specification's
// "validate config before starting background work").
func CheckOwnership(o Ownership) error {
	if !o.Valid() {
		return errOwnership
	}
	return nil
}

var errOwnership = errcode.New(errcode.InvalidConfig,
	"iqmq/driverkit: ownership must be ClientBorrowed or ClientOwned")

// compile-time proof that Ownership answers what a backend needs of it.
var _ = [...]mq.ErrorCode{mq.CodeInvalidConfig}
