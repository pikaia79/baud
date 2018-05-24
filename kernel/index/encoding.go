package index

import (
	"errors"

	"github.com/tiglabs/baudengine/util/encoding"
	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/proto/metapb"
)

type KEY_TYPE byte
type FIELD_TYPE byte

const (
	// field value
	KEY_TYPE_F KEY_TYPE = 'F'
	// raft apply ID
	KEY_TYPE_R KEY_TYPE = 'R'
	// term index
	KEY_TYPE_I KEY_TYPE = 'I'
	// term vector
	KEY_TYPE_V KEY_TYPE = 'V'
	// term position
	KEY_TYPE_P KEY_TYPE = 'P'
	// term entity info
	KEY_TYPE_T KEY_TYPE = 'T'
)

const (
	// unknown field
	FIELD_TYPE_U FIELD_TYPE = 'U'
	// string field
	FIELD_TYPE_S FIELD_TYPE = 'S'
	// int field
	FIELD_TYPE_I FIELD_TYPE = 'I'
	// float field
	FIELD_TYPE_F FIELD_TYPE = 'F'
	// decimal field
	FIELD_TYPE_D FIELD_TYPE = 'D'
	// bool field
	FIELD_TYPE_BOOL FIELD_TYPE = 'b'
	// null field
	FIELD_TYPE_N FIELD_TYPE = 'N'
	// date field
	FIELD_TYPE_T FIELD_TYPE = 'T'
	// blob field
	FIELD_TYPE_B FIELD_TYPE = 'B'
	// geo field
	FIELD_TYPE_G FIELD_TYPE = 'G'
)

func fieldTypeOuter(_type FIELD_TYPE) pspb.ValueType {
	switch _type {
	case FIELD_TYPE_S:
		return pspb.ValueType_STRING
	case FIELD_TYPE_I:
		return pspb.ValueType_INT
	case FIELD_TYPE_F:
		return pspb.ValueType_FLOAT
	case FIELD_TYPE_D:
		return pspb.ValueType_DECIMAL
	case FIELD_TYPE_BOOL:
		return pspb.ValueType_BOOL
	case FIELD_TYPE_N:
		return pspb.ValueType_NULL
	case FIELD_TYPE_T:
		return pspb.ValueType_TIME
	case FIELD_TYPE_B:
		return pspb.ValueType_BLOB
	default:
		// TODO geo
		return pspb.ValueType_UNKNOWN
	}
}

func fieldTypeInner(_type pspb.ValueType) FIELD_TYPE {
	switch _type {
	case pspb.ValueType_STRING:
		return FIELD_TYPE_S
	case pspb.ValueType_INT:
		return FIELD_TYPE_I
	case pspb.ValueType_FLOAT:
		return FIELD_TYPE_F
	case pspb.ValueType_DECIMAL:
		return FIELD_TYPE_D
	case pspb.ValueType_BOOL:
		return FIELD_TYPE_BOOL
	case pspb.ValueType_NULL:
		return FIELD_TYPE_N
	case pspb.ValueType_TIME:
		return FIELD_TYPE_T
	case pspb.ValueType_BLOB:
		return FIELD_TYPE_B
	default:
		return FIELD_TYPE_U
	}
}

// field key format: [type][doc ID][field ID]
func encodeStoreFieldKey(docID []byte, fieldId uint32) (key []byte) {
	key = append(key, byte(KEY_TYPE_F))
	key = encoding.EncodeBytesAscending(key, docID)
	if fieldId > 0 {
		key = encoding.EncodeUint32Ascending(key, fieldId)
	}
	return
}

func decodeStoreFieldKey(key []byte) (docId []byte, fileId uint32, err error) {
	if len(key) <= 2 {
		err = errors.New("invalid field key")
		return
	}
	if key[0] != byte(KEY_TYPE_F) {
		err = errors.New("invalid field key")
		return
	}
	key, docId, err = encoding.DecodeBytesAscending(key[1:], nil)
	if err != nil {
		return
	}

	_, fileId, err = encoding.DecodeUint32Ascending(key)
	if err != nil {
		return
	}
	return
}

// fields must have the same field type
func encodeStoreField(docID []byte, field *pspb.Field) (key []byte, row []byte, err error) {
	if !field.Desc.Stored {
		return nil, nil, nil
	}
	fieldType := fieldTypeInner(field.Type)
	key = encodeStoreFieldKey(docID, field.Id)
	row = append(row, byte(fieldType))
	if len(field.Data) > 0 {
		row = append(row, []byte(field.Data)...)
	}
	return
}

func decodeStoreField(fieldId uint32, row []byte) (*pspb.Field, error) {
	if len(row) == 0 {
		return nil, errors.New("invalid field row")
	}

	fieldType := FIELD_TYPE(row[0])
	field := &pspb.Field{}
	field.Id = fieldId
	field.Type = fieldTypeOuter(fieldType)
	if fieldType == FIELD_TYPE_N {
		return field, nil
	}
	if len(row[1:]) == 0 {
		return nil, errors.New("invalid field row")
	}
	field.Data = metapb.Value(row[1:])
	return field, nil
}

// index key format: [type][field ID][term][doc ID]
func encodeIndexKey(docID []byte, fieldId uint32, term []byte) (key []byte) {
	key = append(key, byte(KEY_TYPE_I))
	key = encoding.EncodeUint32Ascending(key, fieldId)
	key = encoding.EncodeBytesAscending(key, term)
	key = encoding.EncodeBytesAscending(key, docID)
	return
}

// freq == 0 for doc only
func encodeIndex(docID []byte, fieldId uint32, term []byte, freq int) (key []byte, row []byte, err error) {
	key = encodeIndexKey(docID, fieldId, term)
	row = encoding.EncodeIntValue(row, 0, int64(freq))
	return
}

// index position key format: [type][field ID][term][doc ID][pos]
func encodeIndexPositionKey(docID []byte, fieldId uint32, term []byte, pos int) (key []byte) {
	key = append(key, byte(KEY_TYPE_P))
	key = encoding.EncodeUint32Ascending(key, fieldId)
	key = encoding.EncodeBytesAscending(key, term)
	key = encoding.EncodeBytesAscending(key, docID)
	if pos > 0 {
		key = encoding.EncodeUint32Ascending(key, uint32(pos))
	}
	return
}

func encodeIndexPosition(docID []byte, fieldId uint32, term []byte, pos, start, end int) (key []byte, row []byte, err error) {
	key = encodeIndexPositionKey(docID, fieldId, term, pos)
	row = encoding.EncodeIntValue(row, 0, int64(pos))
	if end > 0 {
		// for offset
		row = encoding.EncodeIntValue(row, 1, int64(start))
		row = encoding.EncodeIntValue(row, 2, int64(end))
	}
	return
}

func encodeFieldTermAbstractKey(docID []byte, fieldId uint32) (key []byte) {
	key = append(key, byte(KEY_TYPE_T))
	key = encoding.EncodeBytesAscending(key, docID)
	if fieldId > 0 {
		key = encoding.EncodeUint32Ascending(key, fieldId)
	}
	return
}

func encodeFieldTermAbstract(docID []byte, fieldId uint32, terms [][]byte) (key []byte, row []byte, err error) {
	if len(terms) == 0 {
		err = errors.New("no terms")
		return
	}
	key = encodeFieldTermAbstractKey(docID, fieldId)
	for i, term := range terms {
		row = encoding.EncodeBytesValue(row, uint32(i), term)
	}
	return
}

func decodeFieldTermAbstractKey(key []byte) (docID []byte, fieldId uint32, err error) {
	if len(key) <= 1 || key[0] != byte(KEY_TYPE_T) {
		err = errors.New("invalid field term abstract key")
		return
	}
	key = key[1:]
	key, docID, err = encoding.DecodeBytesAscending(key, nil)
	if err != nil {
		return
	}
	key, fieldId, err = encoding.DecodeUint32Ascending(key)
	return
}

func decodeFieldTermAbstractValue(value []byte) ([][]byte, error) {
	var terms [][]byte

	for len(value) > 0 {
		var term []byte
		var err error
		value, term, err = encoding.DecodeBytesValue(value)
		if err != nil {
			return nil, err
		}
		terms = append(terms, term)
	}
	return terms, nil
}
