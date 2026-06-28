package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// The command-line tool's two places that tell a person whether a failed
// publication may still be in the broker: `bridge`'s ambiguous bucket and
// `pub`'s "NOT CONFIRMED". Both asked their own question -- bridge read the
// *OpError's flag alone, pub the code alone -- where the library has one,
// mq.IsAmbiguous, which reads both. These drive the real
// commands over a sink that answers with ambiguity in the CODE alone, the
// shape drivers/redis's short WAITAOF had.

// codeOnlyAmbiguous is a publication failure ambiguous by its code alone.
var codeOnlyAmbiguous = &mq.OpError{Driver: "seam", Operation: mq.OpPublish, Topic: "mirror", Code: mq.CodeAmbiguous}

// sinkHarness registers one destination, "seamdst", over sink.
func sinkHarness(t *testing.T, sink *fakeSink) *harness {
	t.Helper()
	backend := &seamBackend{caps: seamCaps(mq.FeaturePublisherConfirm, mq.FeatureBatchPublish), sink: sink}
	reg := dsn.NewRegistry()
	if err := reg.Register("seamdst", func(context.Context, string) (*mq.Client, error) {
		return mq.NewClient(backend), nil
	}); err != nil {
		t.Fatalf("registering seamdst: %v", err)
	}
	return harnessWith(t, reg, nil)
}

// TestBridgeCountsAmbiguityCarriedInTheCode: bridge's ambiguousPublish read
// only the flag, so a failure ambiguous by its code was counted as a clean
// refusal and the "MAY already hold this message" warning was not printed.
func TestBridgeCountsAmbiguityCarriedInTheCode(t *testing.T) {
	log := &seamLog{}
	settler := &fakeSettler{log: log}
	source := &fakeSource{log: log, bodies: []string{"a"}, settler: settler, closed: make(chan struct{})}
	src := &seamBackend{caps: seamCaps(mq.FeatureNack, mq.FeatureAtLeastOnce), source: source}
	dst := &seamBackend{
		caps: seamCaps(mq.FeaturePublisherConfirm),
		sink: &fakeSink{log: log, failOn: 1, failErr: codeOnlyAmbiguous},
	}
	reg := dsn.NewRegistry()
	for scheme, b := range map[string]*seamBackend{"seamsrc": src, "seamdst": dst} {
		backend := b
		if err := reg.Register(scheme, func(context.Context, string) (*mq.Client, error) {
			return mq.NewClient(backend), nil
		}); err != nil {
			t.Fatalf("registering %q: %v", scheme, err)
		}
	}
	h := harnessWith(t, reg, nil)
	if code := h.run("bridge", "--max", "1", "-o", "json", "seamsrc://", "orders", "bridge",
		"seamdst://", "mirror"); code != exitFailure {
		t.Fatalf("bridge = %d, want %d\nstderr:\n%s", code, exitFailure, h.stderr())
	}
	var rep bridgeReport
	if err := json.Unmarshal([]byte(h.stdout()), &rep); err != nil {
		t.Fatalf("the JSON form does not parse: %v\n%s", err, h.stdout())
	}
	if rep.Result.Ambiguous != 1 {
		t.Errorf("ambiguous = %d, want 1: the failure's code says its acceptance is unknown", rep.Result.Ambiguous)
	}
	if !strings.Contains(h.stderr(), "MAY") {
		t.Errorf("the failure was not reported as possibly held by the destination:\n%s", h.stderr())
	}
}

// TestPubPrintsNotConfirmedAtEveryOutputSite pins pub's three output sites:
// one publication at a time, a batch answered per message, and a batch
// answered with one error.
func TestPubPrintsNotConfirmedAtEveryOutputSite(t *testing.T) {
	for _, tc := range []struct {
		name string
		sink *fakeSink
		args []string
		want string
	}{
		{
			"one at a time", &fakeSink{log: &seamLog{}, failOn: 1, failErr: codeOnlyAmbiguous},
			[]string{"pub", "seamdst://", "mirror", "--file", "-"},
			"iqmq pub: message 0 NOT CONFIRMED (acceptance unknown)",
		},
		{
			"a batch answered per message", &fakeSink{log: &seamLog{}, failOn: 1, failErr: codeOnlyAmbiguous},
			[]string{"pub", "seamdst://", "mirror", "--file", "-", "--batch"},
			"iqmq pub: message 0 NOT CONFIRMED (acceptance unknown)",
		},
		{
			"a batch answered with one error", &fakeSink{log: &seamLog{}, failAll: true, failErr: codeOnlyAmbiguous},
			[]string{"pub", "seamdst://", "mirror", "--file", "-", "--batch"},
			"iqmq pub: batch NOT CONFIRMED (acceptance unknown)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := sinkHarness(t, tc.sink).withStdin("a\nb\n")
			if code := h.run(tc.args...); code == exitOK {
				t.Fatalf("pub = %d with a failing sink\nstderr:\n%s", code, h.stderr())
			}
			if !strings.Contains(h.stderr(), tc.want) {
				t.Fatalf("pub printed\n%s\nwant a line containing %q", h.stderr(), tc.want)
			}
			if strings.Contains(h.stderr(), "refused") {
				t.Fatalf("an ambiguous publication was reported as refused:\n%s", h.stderr())
			}
		})
	}
}
