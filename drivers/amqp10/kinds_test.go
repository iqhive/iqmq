package amqp10_test

import (
	"strings"
	"testing"

	"bitbucket.org/iqhive/iqmq/driverkit"
	aq "bitbucket.org/iqhive/iqmq/drivers/amqp10"
)

// The typed enumerations, and the refusals the entry points make.
//
// Each enumeration is a value a deployment sets and three methods nothing
// exercised: the name a log line prints, the validity a configuration check
// asks for, and the DEFAULT a zero resolves to. The last is the one that
// matters -- the zero value is what a caller who set nothing gets, and a
// Resolve that answered the wrong thing would change the settlement contract
// of every link without changing what anybody wrote.

// TestEveryKindNamesItselfAndKnowsWhetherItIsOne.
//
// The out-of-range case is a value from a stale build or a bad cast, and
// both answers matter: the name is read by a log line, and Valid is what a
// configuration check asks so that the refusal names the field.
func TestEveryKindNamesItselfAndKnowsWhetherItIsOne(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind    string
		named   []string
		badName string
		badOK   bool
	}{
		{
			kind: "BackoffPolicy",
			named: []string{
				aq.BackoffDefault.String(), aq.BackoffNone.String(), aq.BackoffConstant.String(),
				aq.BackoffFullJitter.String(), aq.BackoffEqualJitter.String(), aq.BackoffDecorrelated.String(),
			},
			badName: aq.BackoffPolicy(99).String(), badOK: aq.BackoffPolicy(99).Valid(),
		},
		{
			kind: "SASLMechanism",
			named: []string{
				aq.SASLAuto.String(), aq.SASLNone.String(), aq.SASLAnonymous.String(),
				aq.SASLPlain.String(), aq.SASLExternal.String(),
			},
			badName: aq.SASLMechanism(99).String(), badOK: aq.SASLMechanism(99).Valid(),
		},
		{
			kind: "SenderSettleMode",
			named: []string{
				aq.SenderSettleDefault.String(), aq.SenderSettleUnsettled.String(),
				aq.SenderSettleSettled.String(), aq.SenderSettleMixed.String(),
			},
			badName: aq.SenderSettleMode(99).String(), badOK: aq.SenderSettleMode(99).Valid(),
		},
		{
			kind: "ReceiverSettleMode",
			named: []string{
				aq.ReceiverSettleDefault.String(), aq.ReceiverSettleFirst.String(),
				aq.ReceiverSettleSecond.String(),
			},
			badName: aq.ReceiverSettleMode(99).String(), badOK: aq.ReceiverSettleMode(99).Valid(),
		},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			t.Parallel()
			seen := map[string]bool{}
			for _, n := range tc.named {
				if n == "" || strings.Contains(n, "(?)") {
					t.Errorf("a %s has no name: %q", tc.kind, n)
				}
				if seen[n] {
					t.Errorf("two %s values are both named %q; a log line could not tell them apart", tc.kind, n)
				}
				seen[n] = true
			}
			if !strings.Contains(tc.badName, "(?)") {
				t.Errorf("%s(99) renders as %q; an out-of-range value has to say so rather than borrow a "+
					"real name", tc.kind, tc.badName)
			}
			if tc.badOK {
				t.Errorf("%s(99) reports itself valid", tc.kind)
			}
		})
	}
}

// TestTheSettleModesResolveTheirZeroToTheProtocolsDefault.
//
// The zero value is what a caller who set nothing gets, and it is not a
// settlement mode: it means "whatever this profile says". Resolve is where
// that becomes a real one, and a wrong answer changes the settlement
// contract of every link this driver opens without changing anything anybody
// wrote.
func TestTheSettleModesResolveTheirZeroToTheProtocolsDefault(t *testing.T) {
	t.Parallel()

	if got := aq.SenderSettleDefault.Resolve(); got == aq.SenderSettleDefault {
		t.Error("SenderSettleDefault resolves to itself; the zero means \"whatever this profile says\" and " +
			"has to become a real mode")
	}
	if got := aq.SenderSettleSettled.Resolve(); got != aq.SenderSettleSettled {
		t.Errorf("SenderSettleSettled resolves to %v; a mode the caller CHOSE must not be overridden", got)
	}

	if got := aq.ReceiverSettleDefault.Resolve(); got == aq.ReceiverSettleDefault {
		t.Error("ReceiverSettleDefault resolves to itself")
	}
	if got := aq.ReceiverSettleSecond.Resolve(); got != aq.ReceiverSettleSecond {
		t.Errorf("ReceiverSettleSecond resolves to %v; a mode the caller chose must not be overridden", got)
	}

	if got := aq.BackoffDefault.Resolve(); got == aq.BackoffDefault {
		t.Error("BackoffDefault resolves to itself; a reconnect loop needs a real policy")
	}
	if got := aq.BackoffNone.Resolve(); got != aq.BackoffNone {
		t.Errorf("BackoffNone resolves to %v; a policy the caller chose must not be overridden", got)
	}
}

// TestTheEntryPointsRefuseWhatTheyCannotUse.
//
// Both take something the CALLER owns -- a connection or a dialer -- so
// there is nothing to default. A nil is a programming error and is named as
// one rather than deferred to a nil dereference on first use.
func TestTheEntryPointsRefuseWhatTheyCannotUse(t *testing.T) {
	t.Parallel()
	profile := testProfile()
	cfg := aq.Config{Addrs: []string{"127.0.0.1:1"}}

	if _, err := aq.BackendWithClient(t.Context(), nil, driverkit.ClientBorrowed, profile, cfg); err == nil {
		t.Error("BackendWithClient accepted a nil connection")
	}
	if _, err := aq.OpenWithClient(t.Context(), nil, driverkit.ClientBorrowed, profile, cfg); err == nil {
		t.Error("OpenWithClient accepted a nil connection")
	}
	if _, err := aq.BackendWithDialer(t.Context(), profile, cfg, nil); err == nil {
		t.Error("BackendWithDialer accepted a nil dialer")
	}
}
