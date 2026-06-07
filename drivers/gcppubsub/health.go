package gcppubsub

import mq "bitbucket.org/iqhive/iqmq"

// Health is the driver's health defines it.
//
// Pub/Sub is a gRPC service the client library reconnects to on its own, so
// there is no dial loop here and no [bitbucket.org/iqhive/iqmq/driverkit.Reconnector]:
// what this reports is what the driver has SEEN. A streaming pull that returned
// a non-retryable error, a publication that failed, an administrative call that
// failed — each moves the state to Degraded, and the next success moves it back
// to Ready.
//
// That is deliberately weaker than a driver that owns its connection, and the
// driver page says so: a client that has never been asked to do anything
// reports Ready because nothing has failed, not because anything has been
// proved. [Backend.Ping] is the call that proves it.
func (b *Backend) Health() mq.HealthSnapshot { return b.health.Snapshot() }
