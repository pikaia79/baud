package document

import (
	"fmt"

	"github.com/tiglabs/baud/document/numeric"
)

/*
数值类型，注意numeric并不是一个类型，它包括多种类型，比如：long,integer,short,byte,double,float，
每种的存储空间都是不一样的，一般默认推荐integer和float。
详细参考elasticsearch官方文档 https://www.elastic.co/guide/en/elasticsearch/reference/current/number.html
*/
type NumericField struct {
	name              string
	value             numeric.PrefixCoded
	numPlainTextBytes uint64
}

func (n *NumericField) Name() string {
	return n.name
}

func (n *NumericField) Value() []byte {
	return n.value
}

func (n *NumericField) Number() (float64, error) {
	i64, err := n.value.Int64()
	if err != nil {
		return 0.0, err
	}
	return numeric.Int64ToFloat64(i64), nil
}

func (n *NumericField) String() string {
	return fmt.Sprintf("&document.NumericField{Name:%s, Value: %s}", n.name, n.value)
}

func (n *NumericField) NumPlainTextBytes() uint64 {
	return n.numPlainTextBytes
}

func NewNumericFieldFromBytes(name string, value []byte) *NumericField {
	return &NumericField{
		name:              name,
		value:             value,
		numPlainTextBytes: uint64(len(value)),
	}
}

func NewNumericField(name string, number float64) *NumericField {
	numberInt64 := numeric.Float64ToInt64(number)
	prefixCoded := numeric.MustNewPrefixCodedInt64(numberInt64, 0)
	return &NumericField{
		name:           name,
		value:          prefixCoded,
		numPlainTextBytes: uint64(8),
	}
}
