package natsunified_test

import (
	"os"
	"regexp"
	"testing"

	natsgo "github.com/nats-io/nats.go"
)

// TestThePageNamesTheLinkedClient holds the facade page's "Native client" row
// to the nats.go both profile drivers link. The facade pins no client of its
// own, so the row is the only place its version is written, and it went on
// naming v1.53.1 after the drivers moved to v1.55.0.
func TestThePageNamesTheLinkedClient(t *testing.T) {
	page, err := os.ReadFile("../../docs/drivers/natsunified.md")
	if err != nil {
		t.Fatalf("reading the page: %v", err)
	}
	m := regexp.MustCompile("\\| Native client \\| `github\\.com/nats-io/nats\\.go` v([0-9.]+)").FindSubmatch(page)
	if m == nil {
		t.Fatal("docs/drivers/natsunified.md has no Native client row")
	}
	if string(m[1]) != natsgo.Version {
		t.Fatalf("docs/drivers/natsunified.md names nats.go v%s; the linked client is v%s", m[1], natsgo.Version)
	}
}
