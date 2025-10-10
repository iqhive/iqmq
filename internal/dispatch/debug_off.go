//go:build !iqmq_debug

package dispatch

// debugWindows reports whether the close-path scheduling seams are compiled
// in. They are not, outside an iqmq_debug build.
const debugWindows = false

// refusalWindow is the ordinary build's empty half of the iqmq_debug
// scheduling seam described in debug.go. It compiles to nothing.
func refusalWindow() {}
