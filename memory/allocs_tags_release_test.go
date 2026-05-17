//go:build !iqmq_debug

package memory_test

// allocsDebugCanary reports that the iqmq_debug canaries are not compiled in,
// which is the build the figures are stated for.
const allocsDebugCanary = false
