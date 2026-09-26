package toydriver_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/examples/custom-driver/toydriver"
)

// TestConfigRendersNoCredential is the rule for a driver's configuration --
// a credential never reaches a log -- held against the example every driver
// author copies.
func TestConfigRendersNoCredential(t *testing.T) {
	cfg := toydriver.Config{MaxMessageBytes: 4242, Credential: credential}

	var logs bytes.Buffer
	slog.New(slog.NewTextHandler(&logs, nil)).Info("toy", "cfg", cfg)
	slog.New(slog.NewJSONHandler(&logs, nil)).Info("toy", "cfg", cfg)

	for name, got := range map[string]string{
		"%v":          fmt.Sprintf("%v", cfg),
		"%+v":         fmt.Sprintf("%+v", cfg),
		"%#v":         fmt.Sprintf("%#v", cfg),
		"slog (both)": logs.String(),
	} {
		if strings.Contains(got, credential) {
			t.Errorf("%s renders the credential: %s", name, got)
		}
		// A rendering that printed nothing would pass the check above; the
		// other fields have to survive for the test to say anything.
		if !strings.Contains(got, "4242") {
			t.Errorf("%s does not render MaxMessageBytes, so it proves nothing about the field beside it: %s", name, got)
		}
	}
}
