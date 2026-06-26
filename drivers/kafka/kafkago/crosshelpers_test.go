package kafkago_test

import (
	segkafka "github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kerr"
)

// kafkaGoError and franzError are the same protocol error code as each client
// spells it, which is what makes the two drivers' classifications comparable
// at all: neither client's error VALUE is the other's, and the code is the
// only thing they share.
func kafkaGoError(code int16) error { return segkafka.Error(code) }

func franzError(code int16) error { return kerr.ErrorForCode(code) }
