package encrypt_test

import (
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/x/encrypt"
	"github.com/iqhive/iqmq/x/encrypt/encrypttest"
)

// TestContract runs x/encrypt/encrypttest against every suite this module can
// construct. The ChaCha20-Poly1305 module runs the same function against its
// suite, which is what "held to the same tests" means.
func TestContract(t *testing.T) {
	for _, s := range encrypt.NewSuites().List() {
		t.Run(s.String(), func(t *testing.T) {
			encrypttest.Run(t, encrypttest.Harness{Suite: s})
		})
	}
}

// brokenEnv names the defect the child process registers. It is read by
// TestTheSuiteContractAgainstABrokenAEAD, which is the subject rather than a
// test.
const brokenEnv = "IQMQ_ENCRYPTTEST_DEFECT"

// defects maps each deliberately broken AEAD to the contract case that must
// catch it. "none" is a correct AEAD and must pass everything.
var defects = map[string][]string{
	"none":          nil,
	"no-tag-check":  {"TamperingFailsClosed", "TamperingFailsClosedThroughTheMiddleware"},
	"ignores-aad":   {"HeaderIsInTheAuthenticatedData", "AdditionalDataMustMatch"},
	"ignores-nonce": {"TwoSealsDiffer"},
}

// TestTheSuiteContractCanFail is the check on the check.
//
// encrypttest is the instrument the ChaCha20-Poly1305 suite is certified with,
// so it is part of the code under test: a contract that passed a broken AEAD
// would make that suite's green a statement about nothing. This re-executes
// the test binary once per defect, registers an AEAD carrying it under
// SuiteChaCha20Poly1305 in the child -- the root module cannot import the real
// primitive, and that is the suite the contract exists for -- and requires the
// named case to FAIL, and the correct AEAD to pass.
func TestTheSuiteContractCanFail(t *testing.T) {
	if os.Getenv(brokenEnv) != "" {
		t.Skip("this process IS the child; TestTheSuiteContractAgainstABrokenAEAD is the subject")
	}
	for defect, mustFail := range defects {
		t.Run(defect, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), os.Args[0],
				"-test.run=^TestTheSuiteContractAgainstABrokenAEAD$", "-test.v", "-test.count=1")
			cmd.Env = append(os.Environ(), brokenEnv+"="+defect)
			raw, err := cmd.CombinedOutput()
			out := string(raw)
			passed := err == nil
			if passed != (len(mustFail) == 0) {
				t.Fatalf("the contract passed=%v against the AEAD whose defect is %q, want the opposite:\n%s", passed, defect, out)
			}
			if len(mustFail) == 0 {
				if !strings.Contains(out, "--- PASS: TestTheSuiteContractAgainstABrokenAEAD") {
					t.Fatalf("the correct AEAD's run did not report the contract passing, so it may not have run:\n%s", out)
				}
				return
			}
			// A failure is not enough: it has to be the RIGHT one. A child
			// that failed because it could not register its AEAD would look
			// the same from here.
			for _, c := range mustFail {
				if !strings.Contains(out, "--- FAIL: TestTheSuiteContractAgainstABrokenAEAD/"+c+" ") {
					t.Errorf("the run failed, but not in case %s, so the failure may not be the defect's:\n%s", c, out)
				}
			}
		})
	}
}

// TestTheSuiteContractAgainstABrokenAEAD is the child process's whole job, and
// is skipped in an ordinary run: TestTheSuiteContractCanFail runs it, cannot
// be skipped, and fails if its verdict is wrong.
func TestTheSuiteContractAgainstABrokenAEAD(t *testing.T) {
	defect := os.Getenv(brokenEnv)
	if defect == "" {
		t.Skip(brokenEnv + " is unset: TestTheSuiteContractCanFail runs this, in a child process, once per defect")
	}
	if _, ok := defects[defect]; !ok {
		t.Fatalf("unknown defect %q", defect)
	}
	suites := encrypt.NewSuites()
	if err := suites.Register(encrypt.SuiteChaCha20Poly1305, func(key []byte) (cipher.AEAD, error) {
		return newBroken(defect, key)
	}); err != nil {
		t.Fatal(err)
	}
	encrypttest.Run(t, encrypttest.Harness{Suites: suites, Suite: encrypt.SuiteChaCha20Poly1305})
}

// newBroken returns AES-256-GCM (the same sizes as ChaCha20-Poly1305's table
// row) carrying one defect.
func newBroken(defect string, key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return brokenAEAD{AEAD: gcm, block: block, defect: defect}, nil
}

type brokenAEAD struct {
	cipher.AEAD
	block  cipher.Block
	defect string
}

var zeroNonce = make([]byte, 12)

func (b brokenAEAD) Seal(dst, nonce, plaintext, aad []byte) []byte {
	switch b.defect {
	case "ignores-aad":
		aad = nil
	case "ignores-nonce":
		nonce = zeroNonce
	}
	return b.AEAD.Seal(dst, nonce, plaintext, aad)
}

func (b brokenAEAD) Open(dst, nonce, ciphertext, aad []byte) ([]byte, error) {
	switch b.defect {
	case "ignores-aad":
		aad = nil
	case "ignores-nonce":
		nonce = zeroNonce
	case "no-tag-check":
		// GCM's keystream for the data starts at counter 2 of the 12-byte
		// nonce, so CTR from there decrypts it exactly -- without ever
		// looking at the tag.
		if len(ciphertext) < b.Overhead() {
			return nil, errShort
		}
		iv := make([]byte, 16)
		copy(iv, nonce)
		iv[15] = 2
		body := ciphertext[:len(ciphertext)-b.Overhead()]
		out := make([]byte, len(body))
		cipher.NewCTR(b.block, iv).XORKeyStream(out, body)
		return append(dst, out...), nil
	}
	return b.AEAD.Open(dst, nonce, ciphertext, aad)
}

var errShort = errors.New("ciphertext shorter than the tag")

// TestRegisterSuiteRefusals: only a defined suite with no built-in primitive
// can be registered, only once per registry, never with a nil constructor and
// never into a nil registry.
func TestRegisterSuiteRefusals(t *testing.T) {
	gcm := func(key []byte) (cipher.AEAD, error) { return newBroken("none", key) }
	for name, c := range map[string]struct {
		suite encrypt.Suite
		ctor  func([]byte) (cipher.AEAD, error)
	}{
		"a built-in suite": {encrypt.SuiteAES256GCM, gcm},
		"the zero suite":   {encrypt.SuiteUnknown, gcm},
		"an undefined id":  {encrypt.Suite(200), gcm},
		"nil constructor":  {encrypt.SuiteChaCha20Poly1305, nil},
	} {
		if err := encrypt.NewSuites().Register(c.suite, c.ctor); !errors.Is(err, encrypt.ErrInvalidConfig) {
			t.Errorf("%s: Register = %v, want ErrInvalidConfig", name, err)
		}
	}
	var none *encrypt.Suites
	if err := none.Register(encrypt.SuiteChaCha20Poly1305, gcm); !errors.Is(err, encrypt.ErrInvalidConfig) {
		t.Errorf("Register into a nil registry = %v, want ErrInvalidConfig", err)
	}
	once := encrypt.NewSuites()
	if err := once.Register(encrypt.SuiteChaCha20Poly1305, gcm); err != nil {
		t.Fatalf("first Register = %v", err)
	}
	if err := once.Register(encrypt.SuiteChaCha20Poly1305, gcm); !errors.Is(err, encrypt.ErrInvalidConfig) {
		t.Errorf("second Register = %v, want ErrInvalidConfig", err)
	}
	if err := encrypt.NewSuites().Register(encrypt.SuiteChaCha20Poly1305, gcm); err != nil {
		t.Errorf("Register into a second registry = %v, want nil: registries share nothing", err)
	}
}

// TestAnUnregisteredSuiteParsesButDoesNotOpen: a consumer without the
// ChaCha20-Poly1305 module still PARSES an envelope that names it -- the
// layout needs no primitive -- and cannot build a key for it, so such a
// message reads as a deployment problem (ErrNoKey, ErrUnknownSuite) and never
// as a corrupt one.
func TestAnUnregisteredSuiteParsesButDoesNotOpen(t *testing.T) {
	if _, err := encrypt.NewSuites().NewAEAD(encrypt.SuiteChaCha20Poly1305, key(32, 1)); !errors.Is(err, encrypt.ErrUnknownSuite) {
		t.Errorf("NewAEAD for an unregistered suite = %v, want ErrUnknownSuite", err)
	}
	if _, err := encrypt.NewKeyring(nil, encrypt.SuiteChaCha20Poly1305, key(32, 1), "k"); !errors.Is(err, encrypt.ErrUnknownSuite) {
		t.Errorf("NewKeyring for an unregistered suite = %v, want ErrUnknownSuite", err)
	}
	if got := encrypt.SuiteChaCha20Poly1305.KeyLen(); got != 32 {
		t.Errorf("KeyLen = %d, want 32 whether or not the primitive is registered", got)
	}
	env := append([]byte("IQE1\x03\x00\x01k"), make([]byte, 12+16+5)...)
	e, err := encrypt.ParseEnvelope(env)
	if err != nil || e.Suite != encrypt.SuiteChaCha20Poly1305 {
		t.Fatalf("ParseEnvelope of a ChaCha20-Poly1305 envelope = (%v, %v)", e.Suite, err)
	}
	r := rings(t)[encrypt.SuiteAES256GCM]
	o, err := encrypt.NewOpener(encrypt.Config{Keys: r})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.Open(t.Context(), env); !errors.Is(err, encrypt.ErrNoKey) {
		t.Errorf("Open of an envelope naming an unregistered suite = %v, want ErrNoKey", err)
	}
	if got := encrypt.SuiteChaCha20Poly1305.String(); got != "ChaCha20-Poly1305" {
		t.Errorf("String = %q", got)
	}
}
