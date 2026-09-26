package iqmq

import (
	"context"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/lifecycle"
)

// CompiledPublishOptions is a resolved, validated option set bound to the
// publisher that compiled it.
//
// It exists for the hot loop that publishes many messages under one fixed set
// of controls. A publication that names options has its set resolved and
// validated per call — cheap, and zero-allocation because the resolved set
// comes from a pool — while a compiled set is resolved and validated once and
// handed to the driver as it is, so the publication performs no fold, no
// capability check on the controls and no pooling at all.
//
// It is immutable once compiled and safe to share between goroutines. It is
// opaque: a caller that wants to change a control compiles another set.
type CompiledPublishOptions struct {
	// owner is the publisher this set was compiled against, compared by
	// identity on every use. See PublishCompiled.
	owner *PublisherHandle
	// opts is the resolved set, handed to the driver by address. It is never
	// written after CompileOptions returns, which is what makes handing the
	// same address to concurrent publications sound.
	opts PublishOptions
}

// Options returns the option set this was compiled from.
func (c *CompiledPublishOptions) Options() PublishOptions { return c.opts }

// Defaults are the publication controls every publish through this handle
// starts from: PublisherConfig.Defaults, as the publisher resolved it.
//
// It is the set CompileOptions is usually given, because a compiled set is a
// complete set and not a delta.
func (p *PublisherHandle) Defaults() PublishOptions { return p.cfg.Defaults }

// CompileOptions resolves and validates one option set against this
// publisher, once.
//
// o is the complete set of controls the publication will carry, not a delta
// over PublisherConfig.Defaults: a compiled set is what the driver is handed,
// so it has to say everything about the publication. A caller that wants to
// start from the publisher's own defaults compiles Defaults() with its
// changes applied.
//
// A control the backend cannot honour fails here, before any message exists
// and therefore before any I/O — which is the rule, met once for the
// whole loop instead of once per publication.
func (p *PublisherHandle) CompileOptions(o PublishOptions) (*CompiledPublishOptions, error) {
	if p.life.Closing() {
		return nil, ErrClosed
	}
	if err := p.checkOptions(&o); err != nil {
		return nil, err
	}
	return &CompiledPublishOptions{owner: p, opts: o}, nil
}

// PublishCompiled publishes m under a compiled option set.
//
// It takes no per-call options, and that is the point: a compiled set cannot
// be made to carry a control it was not compiled with, because there is
// nowhere to put one. A caller that needs a different control compiles a
// different set.
//
// c must have been compiled by this publisher. A set compiled elsewhere is
// refused with CodeInvalidArgument, whether or not the other publisher's
// driver could honour its controls, and the refusal is a publish error rather
// than something CompileOptions could have caught: a compiled set carries the
// capability answers *and the defaults* of the publisher that compiled it, so
// using it on another publisher would silently publish under a different
// publisher's configuration. The check is one pointer comparison.
func (p *PublisherHandle) PublishCompiled(ctx context.Context, m *Message, c *CompiledPublishOptions) error {
	if m == nil {
		return errNilMessage
	}
	if c == nil {
		return errNilCompiled
	}
	if c.owner != p {
		return errCompiledOtherPublisher
	}
	t := p.life.Enter(lifecycle.OpPublish)
	if !t.OK() {
		return ErrClosed
	}
	defer p.life.Leave(t)
	if err := p.checkMessage(m); err != nil {
		return err
	}
	p.stamp(m)
	p.resetReceipt(m)
	bytes := int64(len(m.Body))
	start, seq := p.begin(ctx, 1, bytes)
	err := p.sink.Publish(ctx, m, &c.opts)
	if err != nil {
		p.counters.failed.Add(1)
	} else {
		p.counters.published.Add(1)
	}
	p.finish(ctx, start, seq, 1, bytes, err)
	return err
}

// The compiled-option failures. The specification names no sentinel for them, so a caller
// compares CodeOf(err) against CodeInvalidArgument.
var (
	errNilCompiled            = errcode.New(errcode.InvalidArgument, "iqmq: compiled publish options are nil")
	errCompiledOtherPublisher = errcode.New(errcode.InvalidArgument, "iqmq: compiled publish options belong to another publisher")
)
