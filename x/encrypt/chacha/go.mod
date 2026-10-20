// The ChaCha20-Poly1305 suite is its own module so that the root module stays
// stdlib-only: x/encrypt defines the suite's id, sizes and envelope with no
// dependencies at all, and only this module links golang.org/x/crypto.
module github.com/iqhive/iqmq/x/encrypt/chacha

go 1.27.0

require (
	github.com/iqhive/iqmq v1.0.2
	golang.org/x/crypto v0.58.0
)

require golang.org/x/sys v0.49.0 // indirect
