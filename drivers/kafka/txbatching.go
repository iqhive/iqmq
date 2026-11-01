package kafka

import (
	"reflect"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// errBatchingTransactional refuses a publisher with core batching on a
// transactional producer.
//
// Such a publisher holds a PublishAsync in the core's buffer, and the batch
// reaches the producer after the call has returned. franz-go puts a record in
// whatever transaction is open when the record arrives, so the batch missed
// the commit of the transaction it was published in, and franz-go refused it
// as produced outside a transaction; or it joined the next transaction, and
// was lost with it when that one aborted, while its callback reported it
// published. franz-go batches per partition itself, so the core's batching
// adds little on Kafka.
var errBatchingTransactional = errcode.New(errcode.InvalidConfig,
	"iqmq/kafka: a publisher with PublisherConfig.Batching cannot publish through a transactional producer, because its batch reaches the producer after the call and so misses the transaction it was published in; open the publisher without Batching (Config.Linger batches in franz-go)")

// batching reports a sink whose publisher batches in the core
// (mq.SinkConfig.Batching).
//
// The field is new in root v1.2.0, and this module is also built against
// v1.1.1, which has no such field, so it is read by name. Against v1.1.1 it
// reads false: that release never tells a sink its publisher batches. Once
// go.mod requires v1.2.0 this is cfg.Batching.
func batching(cfg mq.SinkConfig) bool {
	f := reflect.ValueOf(cfg).FieldByName("Batching")
	return f.IsValid() && f.Kind() == reflect.Bool && f.Bool()
}
