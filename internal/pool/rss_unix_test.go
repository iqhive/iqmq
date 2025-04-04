//go:build unix

package pool

import "syscall"

// maxRSSMB returns the process's peak resident set size in MiB.
func maxRSSMB() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return float64(ru.Maxrss) / 1024 // Linux reports KiB
}
