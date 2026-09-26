package iqmq

import (
	"strings"

	"github.com/iqhive/iqmq/internal/errcode"
)

// requirementKind is which of the two requirement forms a Requirement is.
type requirementKind uint8

const (
	// requireNone is the zero value and names no form. A zero Requirement is
	// therefore invalid rather than vacuously satisfied: a requirement list
	// sized ahead of the requirements that fill it would otherwise pass
	// silently, which is the opposite of the specification's "fail fast".
	requireNone requirementKind = iota
	// requireAtLeast is the ordinal form.
	requireAtLeast
	// requireAny is the categorical form.
	requireAny
)

// levelSet is an acceptance set of levels, one bit per level.
type levelSet uint8

// with returns the set with l added. An invalid level adds nothing.
func (s levelSet) with(l Level) levelSet {
	if !l.Valid() {
		return s
	}
	return s | 1<<l
}

// has reports whether l is in the set.
func (s levelSet) has(l Level) bool { return l.Valid() && s&(1<<l) != 0 }

// Requirement is one construction-time capability requirement.
// Requirements fail fast: a publisher or consumer whose requirements the
// client's capabilities do not meet is never constructed.
//
// A Requirement is a small value type, asks of every option, so a
// requirement list costs nothing at the call site. Both of the forms ship:
// RequireAtLeast is ordinal and compares Level.Rank, RequireAny is categorical
// and names the levels it accepts.
//
// The zero Requirement is invalid rather than vacuously satisfied, and so is
// an acceptance set with nothing in it: both accept whatever they are given,
// which is never what a caller meant.
type Requirement struct {
	feature Feature
	kind    requirementKind
	atLeast Level
	accept  levelSet
}

// RequireAtLeast requires the feature's support to rank at least as high as
// min, by the documented ordinal Level.Rank gives.
//
// Volatile emulation never satisfies a durable requirement whatever the ranks
// say, so RequireAtLeast(f, EmulatedDurable) is not met by
// EmulatedVolatile support.
func RequireAtLeast(f Feature, min Level) Requirement {
	return Requirement{feature: f, kind: requireAtLeast, atLeast: min}
}

// RequireAny requires the feature's support to be one of the levels named. It
// is the non-ordinal form, for an acceptance set that is not a threshold.
func RequireAny(f Feature, levels ...Level) Requirement {
	r := Requirement{feature: f, kind: requireAny}
	for _, l := range levels {
		r.accept = r.accept.with(l)
	}
	return r
}

// Feature returns the feature the requirement is about.
func (r Requirement) Feature() Feature { return r.feature }

// String renders the requirement the way a caller wrote it. The zero
// Requirement renders as such rather than as a requirement of nothing.
func (r Requirement) String() string {
	if r.kind == requireNone {
		return "Requirement{}"
	}
	if r.kind == requireAtLeast {
		return "RequireAtLeast(" + r.feature.String() + ", " + r.atLeast.String() + ")"
	}
	var b strings.Builder
	b.WriteString("RequireAny(")
	b.WriteString(r.feature.String())
	for l := Unsupported; l < levelCount; l++ {
		if r.accept.has(l) {
			b.WriteString(", ")
			b.WriteString(l.String())
		}
	}
	b.WriteString(")")
	return b.String()
}

// valid reports whether the requirement can be evaluated at all.
func (r Requirement) valid() bool {
	if !r.feature.Valid() {
		return false
	}
	switch r.kind {
	case requireAtLeast:
		return r.atLeast.Valid()
	case requireAny:
		return r.accept != 0
	case requireNone:
		return false
	}
	return false
}

// satisfiedBy reports whether support s meets the requirement.
// It is only ever reached for a valid requirement, so requireNone cannot
// arrive here.
func (r Requirement) satisfiedBy(s Support) bool {
	if r.kind == requireAtLeast {
		if r.atLeast.Durable() && !s.Level.Durable() {
			// The specification: volatile emulation NEVER satisfies a durability
			// requirement, whatever the ranks say.
			return false
		}
		return s.Level.Rank() >= r.atLeast.Rank()
	}
	return r.accept.has(s.Level)
}

// Require evaluates every requirement against the capabilities and returns the
// first failure, or nil when all of them are met.
//
// A failure carries CodeUnsupported, so errors.Is(err, ErrUnsupported) is
// true, and names the requirement and the support that did not meet it —
// including the condition a conditional support stated, which is what makes
// the specification's "conditional support names and tests its condition" checkable by a
// caller. A requirement that cannot be evaluated at all — an unknown feature,
// an unknown level, an empty acceptance set — carries CodeInvalidArgument.
func (c Capabilities) Require(reqs ...Requirement) error {
	for _, r := range reqs {
		if !r.valid() {
			return errInvalidCapability
		}
		got := c.Support(r.feature)
		if !r.satisfiedBy(got) {
			return &requirementError{req: r, got: got}
		}
	}
	return nil
}

// errInvalidCapability reports a feature, level, scope or requirement that
// cannot be evaluated. Like the header failures it has no exported name:
// the sentinel list names none for it and errors.Is matches by code.
var errInvalidCapability = errcode.New(errcode.InvalidArgument, "iqmq: invalid capability requirement")

// requirementError is the failure of one requirement. It is unexported
// because the error surface is the code, the sentinels and *OpError; a
// driver that wants to report the failure with its own context wraps this one
// in an *OpError with CodeUnsupported.
type requirementError struct {
	req Requirement
	got Support
}

// Error implements error. It names the requirement, the level that did not
// meet it and the condition the driver stated for a conditional support.
func (e *requirementError) Error() string {
	msg := "iqmq: " + e.req.String() + " not met: " + e.got.Level.String()
	if e.got.Note != "" {
		msg += " (" + e.got.Note + ")"
	}
	return msg
}

// Code returns CodeUnsupported, so errors.Is matches ErrUnsupported and every
// other error carrying that code.
func (e *requirementError) Code() errcode.Code { return errcode.Unsupported }

// Unwrap returns ErrUnsupported so that a caller may compare either way.
func (e *requirementError) Unwrap() error { return ErrUnsupported }
