//go:build !unix

package pool

// maxRSSMB is unavailable on this platform.
func maxRSSMB() float64 { return 0 }
