package document

import (
	"fmt"
	"math"
	"time"

	"github.com/tiglabs/baud/document/numeric"
)

var MinTimeRepresentable = time.Unix(0, math.MinInt64)
var MaxTimeRepresentable = time.Unix(0, math.MaxInt64)

type DateTimeField struct {
	name              string
	arrayPositions    []uint64
	value             numeric.PrefixCoded
	numPlainTextBytes uint64
}

func (n *DateTimeField) Name() string {
	return n.name
}

func (n *DateTimeField) Value() []byte {
	return n.value
}

func (n *DateTimeField) DateTime() (time.Time, error) {
	i64, err := n.value.Int64()
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, i64).UTC(), nil
}

func (n *DateTimeField) String() string {
	return fmt.Sprintf("&document.DateField{Name:%s, Value: %s}", n.name, n.value)
}

func (n *DateTimeField) NumPlainTextBytes() uint64 {
	return n.numPlainTextBytes
}

func NewDateTimeFieldFromBytes(name string, value []byte) *DateTimeField {
	return &DateTimeField{
		name:              name,
		value:             value,
		numPlainTextBytes: uint64(len(value)),
	}
}

func NewDateTimeField(name string, dt time.Time) (*DateTimeField, error) {
	if validTime(dt) {
		dtInt64 := dt.UnixNano()
		prefixCoded := numeric.MustNewPrefixCodedInt64(dtInt64, 0)
		return &DateTimeField{
			name:           name,
			value:          prefixCoded,
			numPlainTextBytes: uint64(8),
		}, nil
	}
	return nil, fmt.Errorf("cannot represent %s in this type", dt)
}

func validTime(dt time.Time) bool {
	if dt.Before(MinTimeRepresentable) || dt.After(MaxTimeRepresentable) {
		return false
	}
	return true
}
