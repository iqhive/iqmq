package snssqs

import (
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// snsAttrs translates the SQS driver's attribute map into SNS's.
//
// The two SDKs generate the SAME struct under two names — a data type, a
// string value and a binary value — because the two services take the same
// attribute model, and raw message delivery maps one to the other unchanged.
// So this is the whole of this driver's publish-side mapping: the
// decisions, the reserved-prefix refusal, the body policy, the envelope and
// the size accounting are [github.com/iqhive/iqmq/drivers/sqs.Encoder]'s, and
// what is left is a field-for-field copy.
//
// It is a copy rather than an unsafe cast on purpose. The two structs are
// identical today and there is nothing that makes them stay identical: a field
// added to one SDK's generated type and not the other's would make a cast
// silently reinterpret memory, and this loop would keep compiling and keep
// being right.
func snsAttrs(in map[string]sqstypes.MessageAttributeValue) map[string]snstypes.MessageAttributeValue {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]snstypes.MessageAttributeValue, len(in))
	for k, v := range in {
		out[k] = snstypes.MessageAttributeValue{
			DataType:    v.DataType,
			StringValue: v.StringValue,
			BinaryValue: v.BinaryValue,
		}
	}
	return out
}

// sqsAttrs is [snsAttrs] backwards: the attribute map a raw-delivery
// subscription puts on the SQS message.
//
// Nothing in the driver's own path calls it — the consume side reads real SQS
// messages — and it is here because it is what MODELS raw message delivery,
// which is what the fake server and FuzzSNSAttributes need in order to close
// the round trip. The model being one function used by both is what makes the
// fuzz target evidence about the driver rather than about itself: if this
// mapping were wrong, the fake would deliver something the emulators do not
// and `conformance/suites` would disagree with the unit tests.
func sqsAttrs(in map[string]snstypes.MessageAttributeValue) map[string]sqstypes.MessageAttributeValue {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]sqstypes.MessageAttributeValue, len(in))
	for k, v := range in {
		out[k] = sqstypes.MessageAttributeValue{
			DataType:    v.DataType,
			StringValue: v.StringValue,
			BinaryValue: v.BinaryValue,
		}
	}
	return out
}
