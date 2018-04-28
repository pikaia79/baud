package index

import (
	"errors"

	"github.com/tiglabs/baudengine/kernel/document"
	"github.com/tiglabs/baudengine/util/encoding"
)

type KEY_TYPE byte
type FIELD_TYPE byte

const (
	KEY_TYPE_F KEY_TYPE = 'F'
)

const (
	FIELD_TYPE_T FIELD_TYPE = 'T'
	FIELD_TYPE_N FIELD_TYPE = 'N'
	FIELD_TYPE_D FIELD_TYPE = 'D'
	FIELD_TYPE_B FIELD_TYPE = 'B'
)

func encodeStoreFieldKey(docID []byte, fieldName string) (key []byte) {
	key = encoding.EncodeBytesAscending(key, append(docID, byte(KEY_TYPE_F)))
	if len(fieldName) > 0 {
		key = encoding.EncodeBytesAscending(key, []byte(fieldName))
	}
	return
}

func decodeStoreFieldKey(key []byte) (string, error) {
	if len(key) <= 2 {
		return "", errors.New("invalid store field key")
	}
	var err error
	key, _, err = encoding.DecodeBytesAscending(key, nil)
	if err != nil {
		return "", err
	}
	// todo check docID and type

	_, fileName, err := encoding.DecodeBytesAscending(key, nil)
	if err != nil {
		return "", err
	}
	return string(fileName), nil
}

// fields must have the same field type
func encodeStoreField(docID []byte, fields []document.Field) (key []byte, row []byte, err error) {
	var fieldType FIELD_TYPE
	for i, field := range fields {
		if !field.Property().IsStored() {
			return
		}
		if fieldType == 0 {
			switch field.(type) {
			case *document.TextField:
				fieldType = FIELD_TYPE_T
			case *document.NumericField:
				fieldType = FIELD_TYPE_N
			case *document.BooleanField:
				fieldType = FIELD_TYPE_B
			case *document.DateTimeField:
				fieldType = FIELD_TYPE_D
			case *document.CompositeField:
				fieldType = FIELD_TYPE_T
			default:
				err = errors.New("invalid field type")
				return
			}
			key = encodeStoreFieldKey(docID, field.Name())
			row = append(row, byte(fieldType), byte(field.Property()))
		}
		row = encoding.EncodeBytesValue(row, uint32(i), field.Value())
	}
	return
}

func decodeStoreField(fieldName string, row []byte) ([]document.Field, error) {
	if len(row) <= 2 {
		return nil, errors.New("invalid field row")
	}
	var fields []document.Field
	var err error
	fieldType := FIELD_TYPE(row[0])
	property := document.Property(row[1])
	row = row[1:]
	for len(row) > 0 {
		var val []byte
		switch fieldType {
		case FIELD_TYPE_D:
			row, val, err = encoding.DecodeBytesValue(row)
			if err != nil {
				return nil, err
			}
			fields = append(fields, document.NewDateTimeFieldByBytes(fieldName, val, property))
		case FIELD_TYPE_B:
			row, val, err = encoding.DecodeBytesValue(row)
			if err != nil {
				return nil, err
			}
			fields = append(fields, document.NewBooleanFieldByBytes(fieldName, val, property))
		case FIELD_TYPE_N:
			row, val, err = encoding.DecodeBytesValue(row)
			if err != nil {
				return nil, err
			}
			fields = append(fields, document.NewNumericFieldFromBytes(fieldName, val, property))
		case FIELD_TYPE_T:
			row, val, err = encoding.DecodeBytesValue(row)
			if err != nil {
				return nil, err
			}
			fields = append(fields, document.NewTextField(fieldName, val, property))
		default:
			return nil, errors.New("invalid field type")
		}
	}
	return fields, nil
}
