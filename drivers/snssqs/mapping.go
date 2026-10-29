package snssqs

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// The two attribute data types [snsAttrs] moves a value between.
const (
	attrTypeString = "String"
	attrTypeBinary = "Binary"
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
// The copy has one exception, which SNS forces. SNS refuses a String value
// holding a character outside the Basic Multilingual Plane (an emoji, say),
// which it calls a surrogate value, and fails the whole publication as
// InvalidParameterValue. The queue service takes such a character, so the
// shared encoder sends it as a String, and here the value becomes a Binary
// attribute holding the same UTF-8 bytes. Raw delivery hands it to the queue
// unchanged, the decoder reads a Binary value back as the bytes it holds,
// and "String" and "Binary" are the same length, so the size the encoder
// accounted still holds.
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
		if aws.ToString(v.DataType) == attrTypeString && outsideBMP(aws.ToString(v.StringValue)) {
			out[k] = snstypes.MessageAttributeValue{
				DataType:    aws.String(attrTypeBinary),
				BinaryValue: []byte(*v.StringValue),
			}
			continue
		}
		out[k] = snstypes.MessageAttributeValue{
			DataType:    v.DataType,
			StringValue: v.StringValue,
			BinaryValue: v.BinaryValue,
		}
	}
	return out
}

// outsideBMP reports whether s holds a character outside the Basic
// Multilingual Plane, which is one UTF-16 needs a surrogate pair for.
func outsideBMP(s string) bool {
	for _, r := range s {
		if r > 0xFFFF {
			return true
		}
	}
	return false
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
