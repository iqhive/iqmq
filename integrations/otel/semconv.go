package otel

import (
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// SemconvVersion is the OpenTelemetry semantic-convention version this
// package emits, and it is pinned (ADR-024).
//
// A span attribute that silently renames breaks every dashboard, alert and
// saved query built on it, and the messaging conventions have renamed
// attributes across releases — `messaging.destination` became
// `messaging.destination.name`, `messaging.operation` became
// `messaging.operation.type` with a narrowed value set, and
// `messaging.kafka.consumer.group` became the system-independent
// `messaging.consumer.group.name`. So the import is the versioned package
// `go.opentelemetry.io/otel/semconv/v1.43.0` and never
// `.../semconv/vX` chosen by the module's own release: upgrading the OTel API
// module must not be able to rename an attribute.
//
// Moving to a later version is a deliberate change with an ADR entry, and
// [SchemaURL] is set on the tracer and the meter so a collector can apply the
// upstream schema transformations when this version changes.
const SemconvVersion = "1.43.0"

// SchemaURL is the schema URL of [SemconvVersion], attached to every tracer
// and meter this package creates.
const SchemaURL = semconv.SchemaURL

// Operation names. They are the four spans, reported as
// `messaging.operation.name` — the convention's slot for "the system's own
// name for the operation" — while `messaging.operation.type` carries the
// pinned enumeration, which has no "publish" and calls it "send".
const (
	// OperationPublish is the publish span. Its operation.type is
	// "send".
	OperationPublish = "publish"
	// OperationReceive is the receive span.
	OperationReceive = "receive"
	// OperationProcess is the process span.
	OperationProcess = "process"
	// OperationSettle is the settle span.
	OperationSettle = "settle"
)

// opKind is one of the four operations, as an index into the cached
// per-operation state.
type opKind uint8

const (
	opPublish opKind = iota
	opReceive
	opProcess
	opSettle

	opCount
)

// opNames is the operation.name of each kind.
var opNames = [opCount]string{
	OperationPublish, OperationReceive, OperationProcess, OperationSettle,
}

// opTypes is the pinned `messaging.operation.type` of each kind. Publishing
// is "send": the convention's enumeration is create/send/receive/process/
// settle and has no "publish" member, so the four span names and the
// convention's five types are related but not equal.
var opTypes = [opCount]attribute.KeyValue{
	semconv.MessagingOperationTypeSend,
	semconv.MessagingOperationTypeReceive,
	semconv.MessagingOperationTypeProcess,
	semconv.MessagingOperationTypeSettle,
}

// opSpanKinds is the span kind of each operation. A publication is a
// PRODUCER; a fetch and a handler run are CONSUMER; a settlement is a CLIENT
// call to the broker that is neither producing nor consuming a message.
var opSpanKinds = [opCount]trace.SpanKind{
	trace.SpanKindProducer,
	trace.SpanKindConsumer,
	trace.SpanKindConsumer,
	trace.SpanKindClient,
}

// settleOpNames is the operation NAME of a settlement, by mq.SettleKind.
//
// The convention separates `messaging.operation.name` — "the system's own
// name for the operation" — from the pinned `messaging.operation.type`
// enumeration, and a settlement is where the separation earns its keep: all
// three are type "settle", and an ack, a nack and a lease extension are three
// different things a reader needs to tell apart. So the name is the
// settlement's own and the type is "settle", which also makes the span name
// "ack orders" rather than three spans called "settle orders".
var settleOpNames = [3]string{"ack", "nack", "extend"}

// operationName is the `messaging.operation.name` of one operation: the
// settlement's own name for a settle, and the span name otherwise.
func operationName(op opKind, variant uint8) string {
	if op == opSettle && int(variant) < len(settleOpNames) {
		return settleOpNames[variant]
	}
	return opNames[op]
}

// spanName is the convention's span name: the operation's own name and the
// destination, "publish orders". The specification names the four spans `publish`,
// `receive`, `process` and `settle`, and those are the operation names; the
// destination is appended because a span called only "process" cannot be
// grouped by what it processed, which is why the convention requires the
// pair.
func spanName(op opKind, variant uint8, destination string) string {
	name := operationName(op, variant)
	if destination == "" {
		return name
	}
	return name + " " + destination
}

// attrsFor builds the attribute set of one operation on one destination.
//
// It is called once per (operation, topic, subscription) combination and
// cached, so a span start reuses the slice and the option that wraps it. A
// message identifier is never here: the specification forbids message identifiers as
// attributes, because one attribute value per message is how a tracing
// backend's index falls over.
func attrsFor(op opKind, variant uint8, system, destination, group string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 5)
	if system != "" {
		attrs = append(attrs, semconv.MessagingSystemKey.String(system))
	}
	if destination != "" {
		attrs = append(attrs, semconv.MessagingDestinationName(destination))
	}
	attrs = append(attrs,
		opTypes[op],
		semconv.MessagingOperationNameKey.String(operationName(op, variant)),
	)
	if group != "" {
		attrs = append(attrs, semconv.MessagingConsumerGroupName(group))
	}
	return attrs
}

// batchCount is the `messaging.batch.message_count` attribute.
func batchCount(n int) attribute.KeyValue { return semconv.MessagingBatchMessageCount(n) }
