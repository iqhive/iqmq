package iqmq

// Capabilities states only the operations an adapter has explicitly established.
type Capabilities struct{ Durable, ExplicitAck, Nack bool }
type CapabilityProvider interface{ Capabilities() Capabilities }

func (c *Client) Capabilities() Capabilities {
	if p, ok := c.backend.(CapabilityProvider); ok {
		return p.Capabilities()
	}
	return Capabilities{}
}
