package nats

import "errors"

// errorsIsAny reports whether err matches any of the targets.
func errorsIsAny(err error, targets ...error) bool {
	for _, t := range targets {
		if errors.Is(err, t) {
			return true
		}
	}
	return false
}
