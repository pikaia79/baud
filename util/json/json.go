package json

import (
	"bytes"
	"encoding/json"
	"io"
	"time"

	"github.com/json-iterator/go"
	"github.com/json-iterator/go/extra"
)

var jsonAdapter jsoniter.API

func init() {
	extra.RegisterTimeAsInt64Codec(time.Nanosecond)

	jsonAdapter = jsoniter.Config{
		SortMapKeys:             false,
		EscapeHTML:              true,
		ValidateJsonRawMessage:  true,
		MarshalFloatWith6Digits: true,
		UseNumber:               true,
	}.Froze()
}

// Marshal marshal v into valid JSON
func Marshal(v interface{}) ([]byte, error) {
	if m, ok := v.(json.Marshaler); ok {
		return m.MarshalJSON()
	}

	return jsonAdapter.Marshal(v)
}

// MarshalIndent is like Marshal but applies Indent to format the output
func MarshalIndent(v interface{}, prefix, indent string) ([]byte, error) {
	if m, ok := v.(json.Marshaler); ok {
		b, err := m.MarshalJSON()
		if err != nil {
			return nil, err
		}

		var buf bytes.Buffer
		if err = json.Indent(&buf, b, prefix, indent); err == nil {
			return buf.Bytes(), nil
		}
		return nil, err
	}

	return jsonAdapter.MarshalIndent(v, prefix, indent)
}

// Unmarshal unmarshal a JSON data to v
func Unmarshal(data []byte, v interface{}) error {
	if m, ok := v.(json.Unmarshaler); ok {
		return m.UnmarshalJSON(data)
	}

	return jsonAdapter.Unmarshal(data, v)
}

// NewDecoder create decoder read from an input stream
func NewDecoder(r io.Reader) *jsoniter.Decoder {
	return jsonAdapter.NewDecoder(r)
}
