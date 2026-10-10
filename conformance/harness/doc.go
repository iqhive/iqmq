// Package harness starts the broker containers used by the conformance,
// chaos, matrix and soak suites, and knows how to ping each one at the
// protocol level.
//
// Every broker has exactly one constructor, harness.<Broker>(t), that returns
// a running *Broker and registers its termination with t.Cleanup. Constructors
// honour two environment overrides:
//
//   - IQMQ_<BROKER>_ADDR  use an externally managed broker at this address
//     instead of starting a container (IQMQ_<BROKER>_ADDRS may add named
//     secondary endpoints as "name=host:port,name=host:port").
//   - IQMQ_<BROKER>_IMAGE use this image reference instead of the pinned one
//     in images.go (the matrix suite iterates IQMQ_<BROKER>_IMAGES).
//
// <BROKER> is the upper-cased broker name with "-" replaced by "_", for
// example IQMQ_ACTIVEMQ_CLASSIC_ADDR.
//
// The package depends on testcontainers-go and is never imported by the root
// module or by any driver.
package harness
