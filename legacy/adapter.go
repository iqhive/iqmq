package iqmq

import "context"

// FuncBackend adapts native client operations for one configured destination.
type FuncBackend struct {
	PublishFunc func(context.Context, Message) error
	ReceiveFunc func(context.Context) (*Delivery, error)
	CloseFunc   func() error
}

func (b *FuncBackend) Publish(c context.Context, m Message) error {
	if b.PublishFunc == nil {
		return ErrUnsupported
	}
	return b.PublishFunc(c, m)
}
func (b *FuncBackend) Receive(c context.Context) (*Delivery, error) {
	if b.ReceiveFunc == nil {
		return nil, ErrUnsupported
	}
	return b.ReceiveFunc(c)
}
func (b *FuncBackend) Close() error {
	if b.CloseFunc == nil {
		return nil
	}
	return b.CloseFunc()
}
