package document

import (
	"fmt"
	"math"
	"time"
	"errors"

	"github.com/tiglabs/baud/kernel/analysis"
	"github.com/tiglabs/baud/util"
)

const DefaultDateTimeProperty = StoreField | IndexField

var MinTime = time.Unix(0, math.MinInt64)
var MaxTime = time.Unix(0, math.MaxInt64)

var _ Field = &DateTimeField{}

type DateTimeField struct {
	name              string
	property          Property
	value             util.Value
}

func (n *DateTimeField) Name() string {
	return n.name
}

func (n *DateTimeField) Property() Property {
	return n.property
}

func (n *DateTimeField) Analyze() (analysis.TokenFrequencies) {
	tokens := make([]*analysis.Token, 0)
	tokens = append(tokens, &analysis.Token{
		Start:    0,
		End:      len(n.value),
		Term:     n.value,
		Position: 1,
		Type:     analysis.DateTime,
	})

	tokenFreqs := analysis.TokenFrequency(tokens, nil, n.property.IncludeTermVectors())
	return tokenFreqs
}

func (n *DateTimeField) Value() []byte {
	return n.value
}

func (n *DateTimeField) DateTime() (time.Time, error) {
	if len(n.value) == 0 {
		return time.Time{}, errors.New("invalid data time")
	}
	i64, _ := n.value.Int64()
	return time.Unix(0, i64).UTC(), nil
}

func (n *DateTimeField) String() string {
	return fmt.Sprintf("&document.DateField{Name:%s, Property: %s, Value: %s}", n.name, n.property, n.value)
}

func NewDateTimeFieldByBytes(name string, value []byte) *DateTimeField {
	return &DateTimeField{
		name:              name,
		value:             value,
		property:          DefaultDateTimeProperty,
	}
}

func NewDateTimeField(name string, dt time.Time) (*DateTimeField, error) {
	return NewDateTimeFieldWithProperty(name, dt, DefaultDateTimeProperty)
}

func NewDateTimeFieldWithProperty(name string, dt time.Time, property Property) (*DateTimeField, error) {
	if validTime(dt) {
		dtInt64 := dt.UnixNano()
		prefixCoded := util.PrefixCodedInt64(dtInt64, 0)
		return &DateTimeField{
			name:           name,
			value:          prefixCoded,
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
