package main

import (
	"context"

	mq "bitbucket.org/iqhive/iqmq"
	rd "bitbucket.org/iqhive/iqmq/drivers/redis"
)

// rdOpen is the broker arm's constructor, in a file of its own so that
// main_test.go reads as the assertions and not as the wiring.
func rdOpen(ctx context.Context, addr string, hooks mq.Hooks) (*mq.Client, error) {
	return rd.Open(ctx, Config(addr, hooks))
}
