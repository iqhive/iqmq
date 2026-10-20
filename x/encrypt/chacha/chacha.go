package chacha

import (
	"crypto/cipher"

	"golang.org/x/crypto/chacha20poly1305"

	"github.com/iqhive/iqmq/x/encrypt"
)

// KeySize is the suite's key length in bytes.
const KeySize = chacha20poly1305.KeySize

// NewAEAD returns ChaCha20-Poly1305 bound to key, or
// [github.com/iqhive/iqmq/x/encrypt.ErrInvalidConfig] for a key that is not
// [KeySize] bytes.
//
// It is what [Register] hands x/encrypt, and it is exported for a keyring that
// builds its own [github.com/iqhive/iqmq/x/encrypt.Key] -- one backed by a KMS
// that releases key material only into a process it trusts, say. The AEAD is
// safe for concurrent use.
func NewAEAD(key []byte) (cipher.AEAD, error) {
	if len(key) != KeySize {
		return nil, encrypt.ErrInvalidConfig
	}
	return chacha20poly1305.New(key)
}

// Register makes [github.com/iqhive/iqmq/x/encrypt.SuiteChaCha20Poly1305]
// available to every keyring built with s.
//
// Call it at start-up, before a keyring names the suite. It fails -- with
// x/encrypt's ErrInvalidConfig -- for a nil s and when something else
// registered the suite in s first, which is a program linking two primitives
// under one envelope id and is refused rather than resolved by whichever ran
// first.
func Register(s *encrypt.Suites) error {
	return s.Register(encrypt.SuiteChaCha20Poly1305, NewAEAD)
}
