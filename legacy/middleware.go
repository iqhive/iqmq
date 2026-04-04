package iqmq

// Middleware wraps a handler. Chain applies the first argument outermost.
type Middleware func(Handler) Handler

func Chain(handler Handler, middleware ...Middleware) Handler {
	for i := len(middleware) - 1; i >= 0; i-- {
		handler = middleware[i](handler)
	}
	return handler
}
