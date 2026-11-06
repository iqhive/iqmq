package kafka

import "github.com/iqhive/iqmq/internal/errcode"

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
