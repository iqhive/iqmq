package driverkit

// ParkForTest is ClaimOrWait's critical section on its own, so that a test
// can hold the broadcast a waiter would park on and assert which release
// closes it, which a blocking call cannot show.
func (t *TokenTable) ParkForTest(ref any) (uint64, bool, <-chan struct{}) {
	return t.claimOrPark(ref)
}
