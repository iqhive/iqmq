package driverkit_test

import (
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// TestCapabilityBuilderRefusalTexts pins what a driver author reads when the
// builder refuses a conditional support with no condition or an emulation with
// no note. Both texts used to end with a parenthetical citing a document this
// repository does not hold.
func TestCapabilityBuilderRefusalTexts(t *testing.T) {
	for _, tc := range []struct {
		name string
		b    *driverkit.CapabilityBuilder
		want string
	}{
		{
			name: "a conditional support with no condition",
			b:    driverkit.NewCapabilities().Conditional(mq.FeaturePriority, mq.ScopeTopic, ""),
			want: "iqmq/driverkit: a conditional capability must state its condition",
		},
		{
			name: "a volatile emulation with no note",
			b:    driverkit.NewCapabilities().EmulatedVolatile(mq.FeatureReplay, mq.ScopeTopic, ""),
			want: "iqmq/driverkit: an emulated capability must say what the emulation is",
		},
		{
			name: "a durable emulation with no note",
			b:    driverkit.NewCapabilities().EmulatedDurable(mq.FeatureDeadLetter, mq.ScopeTopic, ""),
			want: "iqmq/driverkit: an emulated capability must say what the emulation is",
		},
	} {
		_, err := tc.b.Build()
		if err == nil {
			t.Errorf("%s was accepted", tc.name)
			continue
		}
		if got := err.Error(); got != tc.want {
			t.Errorf("%s: Build = %q, want %q", tc.name, got, tc.want)
		}
	}
}
