//go:build !unix

package wait

import "time"

func cpuTime() time.Duration { return 0 }
