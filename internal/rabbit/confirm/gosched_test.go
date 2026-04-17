package confirm

import "runtime"

// runtimeGosched is runtime.Gosched under a local name, so that the
// concurrency test's intent — widen the interleaving rather than sleep — reads
// at the call site.
func runtimeGosched() { runtime.Gosched() }
