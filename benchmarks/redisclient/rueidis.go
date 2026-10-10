package redisclient

import (
	"github.com/redis/rueidis"

	rd "github.com/iqhive/iqmq/drivers/redis"
)

// newRueidis builds the rueidis arm through the driver's own constructor, with
// the driver's own options.
//
// The options matter as much as the library: `drivers/redis` turns client-side
// caching off (a stream is not read by value, so a cache per connection would
// allocate for nothing) and hands rueidis the key span of every command so
// that a cluster routes it. Measuring rueidis with different options would
// measure a client this repository does not ship.
func newRueidis(addr string) (Client, error) {
	client, err := rueidis.NewClient(rd.ClientOptions(rd.Config{Addrs: []string{addr}}))
	if err != nil {
		return nil, err
	}
	cmder, err := rd.NewCommander(client)
	if err != nil {
		client.Close()
		return nil, err
	}
	return cmder, nil
}
