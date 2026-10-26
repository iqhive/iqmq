package avro

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
