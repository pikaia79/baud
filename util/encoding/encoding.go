package encoding

import (
	"errors"
	"fmt"
)

const (
	intMin      = 0x80 // 128
	intMax      = 0xfd // 253
	intMaxWidth = 8
	intZero     = intMin + intMaxWidth           // 136
	intSmall    = intMax - intZero - intMaxWidth // 109
)

// EncodeUvarintAscending encodes the uint64 value using a variable length (length-prefixed) representation.
func EncodeUvarintAscending(b []byte, v uint64) []byte {
	switch {
	case v <= intSmall:
		return append(b, intZero+byte(v))
	case v <= 0xff:
		return append(b, intMax-7, byte(v))
	case v <= 0xffff:
		return append(b, intMax-6, byte(v>>8), byte(v))
	case v <= 0xffffff:
		return append(b, intMax-5, byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffff:
		return append(b, intMax-4, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffffff:
		return append(b, intMax-3, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8),
			byte(v))
	case v <= 0xffffffffffff:
		return append(b, intMax-2, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),
			byte(v>>8), byte(v))
	case v <= 0xffffffffffffff:
		return append(b, intMax-1, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24),
			byte(v>>16), byte(v>>8), byte(v))
	default:
		return append(b, intMax, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
}

// EncodeUvarintDescending encodes the uint64 value so that it sorts in reverse order, from largest to smallest.
func EncodeUvarintDescending(b []byte, v uint64) []byte {
	switch {
	case v == 0:
		return append(b, intMin+8)
	case v <= 0xff:
		v = ^v
		return append(b, intMin+7, byte(v))
	case v <= 0xffff:
		v = ^v
		return append(b, intMin+6, byte(v>>8), byte(v))
	case v <= 0xffffff:
		v = ^v
		return append(b, intMin+5, byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffff:
		v = ^v
		return append(b, intMin+4, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffffff:
		v = ^v
		return append(b, intMin+3, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8),
			byte(v))
	case v <= 0xffffffffffff:
		v = ^v
		return append(b, intMin+2, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),
			byte(v>>8), byte(v))
	case v <= 0xffffffffffffff:
		v = ^v
		return append(b, intMin+1, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24),
			byte(v>>16), byte(v>>8), byte(v))
	default:
		v = ^v
		return append(b, intMin, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
}

// DecodeUvarintAscending decodes a varint encoded uint64 from the input buffer.
// The remainder of the input buffer and the decoded uint64 are returned.
func DecodeUvarintAscending(b []byte) ([]byte, uint64, error) {
	if len(b) == 0 {
		return nil, 0, errors.New("insufficient bytes to decode uvarint value")
	}
	length := int(b[0]) - intZero
	b = b[1:] // skip length byte
	if length <= intSmall {
		return b, uint64(length), nil
	}
	length -= intSmall
	if length < 0 || length > 8 {
		return nil, 0, fmt.Errorf("invalid uvarint length of %d", length)
	} else if len(b) < length {
		return nil, 0, fmt.Errorf("insufficient bytes to decode uvarint value: %q", b)
	}
	var v uint64
	for _, t := range b[:length] {
		v = (v << 8) | uint64(t)
	}
	return b[length:], v, nil
}

// DecodeUvarintDescending decodes a uint64 value which was encoded using EncodeUvarintDescending.
func DecodeUvarintDescending(b []byte) ([]byte, uint64, error) {
	if len(b) == 0 {
		return nil, 0, errors.New("insufficient bytes to decode uvarint value")
	}
	length := intZero - int(b[0])
	b = b[1:] // skip length byte
	if length < 0 || length > 8 {
		return nil, 0, fmt.Errorf("invalid uvarint length of %d", length)
	} else if len(b) < length {
		return nil, 0, fmt.Errorf("insufficient bytes to decode uvarint value: %q", b)
	}
	var x uint64
	for _, t := range b[:length] {
		x = (x << 8) | uint64(^t)
	}
	return b[length:], x, nil
}
