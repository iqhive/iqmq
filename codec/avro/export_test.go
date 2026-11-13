package avro

import (
	"reflect"

	"github.com/iqhive/iqmq/codec"
)

// The walk's reasons and its depth bound, for this package's tests only. A
// caller has no use for telling the reasons apart (each is CorruptMessage), but
// a test that only asked for CorruptMessage would pass whichever rule refused
// a body, which is the trap reasonIs describes.
var (
	ErrVarintForTest      = errVarint
	ErrValueForTest       = errValue
	ErrLengthForTest      = errLength
	ErrStringLimitForTest = errStringLimit
	ErrBlockForTest       = errBlock
	ErrElementsForTest    = errElements
	ErrZeroWidthForTest   = errZeroWidth
	ErrDepthForTest       = errDepth
)

// MaxRecordDepthForTest is maxRecordDepth.
const MaxRecordDepthForTest = maxRecordDepth

// RepairForTest runs on v, after an Unmarshal into it, the repair Unmarshal
// runs after a decode that prepare says needs it, as if it always did: a
// decode followed by this is what a codec that never skipped the repair would
// leave. Nothing runs between the decode and the repair inside Unmarshal that
// this could change.
func RepairForTest(c, v any) {
	ac, _ := c.(*avroCodec)
	var t any
	var err error
	if ac.topUnion {
		t, err = unionTarget(v)
	} else {
		t, err = codec.Target(v)
	}
	if err != nil {
		return
	}
	got, ok := ac.overwriteChecked.Load(reflect.TypeOf(t))
	if !ok {
		return
	}
	if chk, _ := got.(*typeCheck); chk.err == nil && chk.plan != nil {
		ac.repair(t, chk)
	}
}
