package document

import (
	"fmt"
	"math"
	"time"
	"errors"
	"encoding/binary"

	"github.com/tiglabs/baud/kernel/analysis"
)

const DefaultDateTimeProperty = StoreField | IndexField

var MinTime = time.Unix(0, math.MinInt64)
var MaxTime = time.Unix(0, math.MaxInt64)

var _ Field = &DateTimeField{}

type DateTimeField struct {
	name              string
	arrayPositions    []uint64
	property          Property
	value             []byte
}

func (n *DateTimeField) Name() string {
	return n.name
}

func (n *DateTimeField) ArrayPositions() []uint64 {
	return n.arrayPositions
}

func (n *DateTimeField) Property() Property {
	return n.property
}

func (n *DateTimeField) Analyze() (int, analysis.TokenFrequencies) {
	tokens := make([]*analysis.Token, 0)
	tokens = append(tokens, &analysis.Token{
		Start:    0,
		End:      len(n.value),
		Term:     n.value,
		Position: 1,
		Type:     analysis.DateTime,
	})

	tokenFreqs := analysis.TokenFrequency(tokens, n.arrayPositions, n.property.IncludeTermVectors())
	return tokenFreqs
}

func (n *DateTimeField) Value() []byte {
	return n.value
}

func (n *DateTimeField) DateTime() (time.Time, error) {
	if len(n.value) == 0 {
		return time.Time{}, errors.New("invalid data time")
	}
	i64, _ := binary.Varint(n.value)
	return time.Unix(0, i64).UTC(), nil
}

func (n *DateTimeField) String() string {
	return fmt.Sprintf("&document.DateField{Name:%s, Property: %s, Value: %s}", n.name, n.property, n.value)
}

func NewDateTimeFieldByBytes(name string, arrayPositions []uint64, value []byte) *DateTimeField {
	return &DateTimeField{
		name:              name,
		arrayPositions:    arrayPositions,
		value:             value,
		property:          DefaultDateTimeProperty,
	}
}

func NewDateTimeField(name string, arrayPositions []uint64, dt time.Time) (*DateTimeField, error) {
	return NewDateTimeFieldWithProperty(name, arrayPositions, dt, DefaultDateTimeProperty)
}

func NewDateTimeFieldWithProperty(name string, arrayPositions []uint64, dt time.Time, property Property) (*DateTimeField, error) {
	if validTime(dt) {
		dtInt64 := dt.UnixNano()
		val := make([]byte, 9)
		n := binary.PutVarint(val, dtInt64)
		return &DateTimeField{
			name:           name,
			arrayPositions: arrayPositions,
			value:          val[:n],
			property:       property,
		}, nil
	}
	return nil, fmt.Errorf("invalid time %s", dt)
}

func validTime(dt time.Time) bool {
	if dt.Before(MinTime) || dt.After(MaxTime) {
		return false
	}
	return true
}
