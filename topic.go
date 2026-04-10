package iqmq

import "bitbucket.org/iqhive/iqmq/internal/topicname"

// ValidateTopic reports whether name is a valid portable topic name, returning
// an error carrying CodeInvalidArgument when it is not.
//
// The portable grammar is the intersection every backend can represent; a
// driver may be stricter, and says so on its own page. A name that passes here
// may still be rejected by a driver, and a name that fails here is rejected
// before any I/O.
func ValidateTopic(name string) error { return topicname.Validate(name) }
