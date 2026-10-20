package encrypt

import (
	"context"
	"crypto/cipher"
	"sync"
)

// Key is one key a [Keyring] can seal with.
type Key struct {
	// Suite is the construction.
	Suite Suite
	// ID names the key and is written into every envelope it seals. It is
	// not a secret; it may be empty, which is the "one key, never rotated"
	// case.
	ID string
	// AEAD is the construction bound to the key material. A Keyring returns
	// this rather than raw key bytes so that an implementation backed by a
	// KMS can hand over a handle it constructed and never expose the key.
	AEAD cipher.AEAD
}

// Keyring supplies keys. Implementations must be safe for concurrent use.
//
// The split between [Keyring.Current] and [Keyring.For] is what makes
// rotation possible without a flag day: Current names the key NEW messages
// are sealed with, and For still answers for every key the messages already
// in the broker were sealed with. A keyring that forgets an old key makes
// those messages permanently unopenable, which on a topic with a week of
// retention is a week of dead letters.
type Keyring interface {
	// Current returns the key to seal with.
	Current(ctx context.Context) (Key, error)
	// For returns the AEAD for a suite and key id an envelope named, or
	// [ErrNoKey].
	//
	// It takes the SUITE as well as the id, and both are authenticated, so a
	// keyring that holds one id under two suites during a migration answers
	// correctly for both. A keyring that ignored the suite would hand back
	// an AES-128 AEAD for an AES-256 envelope and the open would fail as
	// ErrNotAuthentic — a confusing way to report a configuration mistake.
	For(ctx context.Context, suite Suite, id string) (cipher.AEAD, error)
}

// StaticKeyring is a [Keyring] over a fixed set of keys: one to seal with and
// any number to open with.
//
// It is safe for concurrent use and it holds key material in process memory,
// which the package documentation lists among the things this package does
// not protect.
type StaticKeyring struct {
	current Key
	// suites builds the AEAD of every key Retire adds.
	suites *Suites
	mu     sync.RWMutex // guards byID; current is immutable
	// byID is keyed on the pair, because one id may exist under two suites
	// during a migration between them.
	byID map[suiteAndID]cipher.AEAD
}

type suiteAndID struct {
	suite Suite
	id    string
}

// NewStaticKeyring returns a keyring holding one key, chosen by its length:
// 16 bytes is AES-128-GCM and 32 is AES-256-GCM. Any other length is
// [ErrInvalidConfig].
func NewStaticKeyring(key []byte, id string) (*StaticKeyring, error) {
	suite, err := SuiteOf(key)
	if err != nil {
		return nil, err
	}
	return NewKeyring(nil, suite, key, id)
}

// NewKeyring returns a keyring holding one key under a named suite. suites
// supplies the primitives of the suites this package cannot construct itself,
// for this key and every key Retire adds; nil knows only the AES-GCM suites.
func NewKeyring(suites *Suites, suite Suite, key []byte, id string) (*StaticKeyring, error) {
	if len(id) > MaxKeyIDLen {
		return nil, ErrKeyIDTooLong
	}
	aead, err := suites.NewAEAD(suite, key)
	if err != nil {
		return nil, err
	}
	return &StaticKeyring{
		current: Key{Suite: suite, ID: id, AEAD: aead},
		suites:  suites,
		byID:    map[suiteAndID]cipher.AEAD{{suite, id}: aead},
	}, nil
}

// Retire adds a key that this keyring will OPEN but never seal with.
//
// It is how a rotation is finished: the new key goes in via
// [NewStaticKeyring] and every key still inside the topic's retention comes
// in here. It returns [ErrInvalidConfig] rather than overwriting a key
// already held under the same suite and id, because a keyring that answered
// differently over its lifetime would make one message openable and the next
// one not.
func (k *StaticKeyring) Retire(suite Suite, key []byte, id string) error {
	if len(id) > MaxKeyIDLen {
		return ErrKeyIDTooLong
	}
	aead, err := k.suites.NewAEAD(suite, key)
	if err != nil {
		return err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, taken := k.byID[suiteAndID{suite, id}]; taken {
		return ErrInvalidConfig
	}
	k.byID[suiteAndID{suite, id}] = aead
	return nil
}

// Current implements [Keyring].
func (k *StaticKeyring) Current(context.Context) (Key, error) { return k.current, nil }

// For implements [Keyring].
func (k *StaticKeyring) For(_ context.Context, suite Suite, id string) (cipher.AEAD, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if aead, ok := k.byID[suiteAndID{suite, id}]; ok {
		return aead, nil
	}
	return nil, ErrNoKey
}

// RotatingKeyring is a [Keyring] whose current key can be replaced while a
// publisher is running, and which keeps every key it has ever held for
// opening.
//
// It exists because the honest alternative is worse: a service that must
// rotate by restarting cannot rotate faster than its deploy cadence, and a
// key's safe message count (see the package documentation's birthday bound)
// is a property of throughput rather than of the release schedule.
type RotatingKeyring struct {
	mu      sync.RWMutex
	current Key
	byID    map[suiteAndID]cipher.AEAD
	// suites builds the AEAD of every key Rotate adds.
	suites *Suites
}

// NewRotatingKeyring returns a rotating keyring seeded with one key. suites
// supplies the primitives of the suites this package cannot construct itself,
// for this key and every key Rotate adds; nil knows only the AES-GCM suites.
func NewRotatingKeyring(suites *Suites, suite Suite, key []byte, id string) (*RotatingKeyring, error) {
	s, err := NewKeyring(suites, suite, key, id)
	if err != nil {
		return nil, err
	}
	return &RotatingKeyring{current: s.current, byID: s.byID, suites: suites}, nil
}

// Rotate makes key the one new messages are sealed with, keeping every
// previous key for opening.
//
// A rotation to an id the keyring already holds under that suite is
// [ErrInvalidConfig]: reusing an id for different key material is the one
// mistake that cannot be detected later, because an envelope names the id and
// nothing else.
func (k *RotatingKeyring) Rotate(suite Suite, key []byte, id string) error {
	if len(id) > MaxKeyIDLen {
		return ErrKeyIDTooLong
	}
	aead, err := k.suites.NewAEAD(suite, key)
	if err != nil {
		return err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, taken := k.byID[suiteAndID{suite, id}]; taken {
		return ErrInvalidConfig
	}
	k.byID[suiteAndID{suite, id}] = aead
	k.current = Key{Suite: suite, ID: id, AEAD: aead}
	return nil
}

// Current implements [Keyring].
func (k *RotatingKeyring) Current(context.Context) (Key, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.current, nil
}

// For implements [Keyring].
func (k *RotatingKeyring) For(_ context.Context, suite Suite, id string) (cipher.AEAD, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if aead, ok := k.byID[suiteAndID{suite, id}]; ok {
		return aead, nil
	}
	return nil, ErrNoKey
}
