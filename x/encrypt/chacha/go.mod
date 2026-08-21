// The ChaCha20-Poly1305 suite is its own module so that the root module stays
// stdlib-only: x/encrypt defines the suite's id, sizes and envelope with no
// dependencies at all, and only this module links golang.org/x/crypto.
module bitbucket.org/iqhive/iqmq/x/encrypt/chacha

go 1.27.0

require (
	bitbucket.org/iqhive/iqmq v0.0.0
	golang.org/x/crypto v0.52.0
)

require golang.org/x/sys v0.46.0 // indirect


replace bitbucket.org/iqhive/iqmq => ../../..
