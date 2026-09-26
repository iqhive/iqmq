package v1compat_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
)

// FuzzV1CompatDSN is the parsing target for this package's one parsing entry
// point, [v1compat.OpenDSN].
//
// `OpenDSN` is `gcppubsub.ParseDSN` followed by `Open`, and the grammar is
// deliberately the v2 driver's: two DSN dialects for one service would be two
// places for a parameter to mean something different. So what this fuzzes is
// the parse, and what it asserts is the three properties a caller of `OpenDSN`
// depends on:
//
//   - it never panics, whatever the string;
//   - a configuration it ACCEPTS is one `Validate` accepts, so `OpenDSN` never
//     reaches `pubsub.NewClient` with a configuration the driver would refuse;
//   - the rendering round-trips through the parser, which is what makes a DSN
//     in a log line something an operator can paste back.
//
// It does NOT call `OpenDSN` itself: the second half of that function builds a
// real client, and for a DSN with no endpoint that means credential discovery
// — a file read and possibly a metadata-server request — which a fuzz target
// must not do.
func FuzzV1CompatDSN(f *testing.F) {
	for _, seed := range []string{
		"gcppubsub://my-project",
		"gcppubsub://my-project?ordering=true&ackdeadline=30s",
		"gcppubsub://127.0.0.1:8085/my-project?insecuretransport=true&autocreate=true",
		"gcppubsub://p?deadlettertopic=dlq&maxdeliveryattempts=5&retainacked=true&retention=10m",
		"gcppubsub://p?prefetch=32&completionworkers=8&completionqueue=256",
		"gcppubsub://p?nonsuch=1",
		"sqs://p",
		"gcppubsub://",
		"",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, in string) {
		cfg, err := gps.ParseDSN(in)
		if err != nil {
			// A refusal must be one classifies, so that a caller can
			// act on it. CodeUnknown would be a failure nobody can handle.
			if code := mq.CodeOf(err); code == mq.CodeUnknown {
				t.Fatalf("ParseDSN(%q) refused with an unclassified error: %v", in, err)
			}
			return
		}
		// An accepted configuration is a usable one: OpenDSN goes straight
		// from here to the client constructor.
		if verr := cfg.Validate(); verr != nil {
			t.Fatalf("ParseDSN(%q) accepted a configuration Validate refuses: %v", in, verr)
		}
		rendered := cfg.DSN()
		if !strings.HasPrefix(rendered, gps.Scheme+"://") {
			t.Fatalf("ParseDSN(%q) renders as %q, which is not a %s DSN", in, rendered, gps.Scheme)
		}
		again, aerr := gps.ParseDSN(rendered)
		if aerr != nil {
			t.Fatalf("the rendering of %q does not parse: %q: %v", in, rendered, aerr)
		}
		// The fields the rendering carries must survive it. The credential is
		// deliberately not among them.
		// Every field but the credential, compared as a whole: naming them
		// one by one is how a field added later stops being checked. The
		// credential is dropped on purpose , so it is
		// cleared on both sides before the comparison.
		want, got := cfg, again
		want.CredentialsJSON, got.CredentialsJSON = "", ""
		want.Clock, got.Clock = nil, nil
		// Compared FIELD BY FIELD through reflect, which is what makes this
		// check survive a field added later. It used to compare `%+v` of the
		// two, and `%+v` of a Config is its String -- a summary that leaves
		// out the filter, every duration and every count -- so a rendering
		// that dropped `ackdeadline` passed even with the seed above that
		// sets it.
		if diffs := fieldDiffs(want, got); len(diffs) > 0 {
			t.Fatalf("the round trip of %q through %q lost a setting: %s", in, rendered, strings.Join(diffs, "; "))
		}
	})
}

// fieldDiffs names every field in which two configurations differ, comparing
// the field values themselves and never the Config's own rendering.
func fieldDiffs(want, got gps.Config) []string {
	vw, vg := reflect.ValueOf(want), reflect.ValueOf(got)
	var diffs []string
	for i := range vw.NumField() {
		if !reflect.DeepEqual(vw.Field(i).Interface(), vg.Field(i).Interface()) {
			diffs = append(diffs, fmt.Sprintf("%s %#v, want %#v", vw.Type().Field(i).Name, vg.Field(i).Interface(), vw.Field(i).Interface()))
		}
	}
	return diffs
}
