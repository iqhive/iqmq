package memory

import mq "github.com/iqhive/iqmq/legacy"

func (b *Backend) Capabilities() mq.Capabilities {
	return mq.Capabilities{ExplicitAck: true, Nack: true}
}
