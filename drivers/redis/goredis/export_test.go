package goredis

import (
	redis "github.com/redis/go-redis/v9"

	"github.com/iqhive/iqmq/internal/streams"
)

// CommanderOverForTest is a commander over any go-redis client, a Ring
// included, for this package's tests only.
//
// The driver refuses a Ring, and a Ring is still the one go-redis client that
// shards by key without a cluster, which is what makes key routing observable
// in a unit test: a command routed at random spreads one stream over two
// servers. The commander does not own the client.
func CommanderOverForTest(client redis.UniversalClient) streams.Commander {
	return &commander{client: client}
}
