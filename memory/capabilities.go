package memory

import mq "bitbucket.org/iqhive/iqmq"

func (b *Backend) Capabilities() mq.Capabilities {
	return mq.Capabilities{ExplicitAck: true, Nack: true}
}
