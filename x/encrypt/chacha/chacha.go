package chacha

import (
	"crypto/cipher"
	"sync"

	"golang.org/x/crypto/chacha20poly1305"

	"bitbucket.org/iqhive/iqmq/x/encrypt"
)

// KeySize is the suite's key length in bytes.
const KeySize = chacha20poly1305.KeySize

// NewAEAD returns ChaCha20-Poly1305 bound to key, or
// [bitbucket.org/iqhive/iqmq/x/encrypt.ErrInvalidConfig] for a key that is not
// [KeySize] bytes.
//
// It is what [Register] hands x/encrypt, and it is exported for a keyring that
// builds its own [bitbucket.org/iqhive/iqmq/x/encrypt.Key] -- one backed by a KMS
// that releases key material only into a process it trusts, say. The AEAD is
// safe for concurrent use.
func NewAEAD(key []byte) (cipher.AEAD, error) {
	if len(key) != KeySize {
		return nil, encrypt.ErrInvalidConfig
	}
	return chacha20poly1305.New(key)
}

var registration struct {
	once sync.Once
	err  error
}

// Register makes [bitbucket.org/iqhive/iqmq/x/encrypt.SuiteChaCha20Poly1305]
// available to every keyring in the process.
//
// Call it at start-up, before a keyring names the suite. It is safe to call
// more than once and from several goroutines: the first call registers, and
// every call returns that first call's result. It fails -- with
// x/encrypt's ErrInvalidConfig -- only when something else registered the
// suite first, which is a process linking two primitives under one envelope
// id and is refused rather than resolved by whichever ran first.
func Register() error {
	registration.once.Do(func() {
		registration.err = encrypt.RegisterSuite(encrypt.SuiteChaCha20Poly1305, NewAEAD)
	})
	return registration.err
}
