package index

import (
	"errors"

	"github.com/tiglabs/baud/kernel/document"
	"github.com/tiglabs/baud/kernel/store/kvstore"
	"github.com/tiglabs/baud/util/encoding"
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

type IndexDriver struct {
	store     kvstore.KVStore
}

func (id *IndexDriver) AddDocuments(docs []*document.Document) error {
	batch := id.store.NewKVBatch()
	for _, doc := range docs {
		// check doc
		val, err := id.store.Get([]byte(doc.ID))
		if err != nil {
			return err
		}
		if val != nil {
			return errors.New("document exist")
		}
		for _, fields := range doc.Fields {
			key, row, err := encodeStoreField(doc.ID, fields)
			if err != nil {
				return err
			}
			batch.Set(key, row)
		}
	}
	return id.store.ExecuteBatch(batch)
}

func (id *IndexDriver) UpdateDocuments(docs []*document.Document) error {
	return nil
}

func (id *IndexDriver) DeleteDocuments(docIDs []string) (int, error) {
	for _, docID := range docIDs {
		// check doc
		val, err := id.store.Get([]byte(docID))
		if err != nil {
			return err
		}
		if val != nil {
			return errors.New("document exist")
		}
		// todo delete document
	}
	return nil
}

// source set true means need return _source
func (id *IndexDriver) GetDocument(docID string, fields []string) (*document.Document, bool) {
	if len(fields) == 0 {
		return nil, false
	}
	var keys [][]byte
	for _, fieldName := range fields {
		keys = append(keys, encodeStoreFieldKey(docID, fieldName))
	}
	values, err := id.store.MultiGet(keys)
	if err != nil {
		return nil, false
	}
	doc := document.NewDocument(docID)
	for i, fieldName := range fields {
		fs, err := decodeStoreField(fieldName, values[i])
		if err != nil {
			return nil, err
		}
		for _, f := range fs {
			doc.AddField(f)
		}
	}
	return doc, nil
}

func encodeStoreFieldKey(docID, fieldName string) (key []byte) {
	key = append(key, KEY_TYPE_F)
	key = encoding.EncodeBytesAscending(key, []byte(docID))
	key = encoding.EncodeBytesAscending(key, []byte(fieldName))
	return
}

// fields must have the same field type
func encodeStoreField(docID string, fields []document.Field) (key []byte, row []byte, err error) {
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
			row = append(row, fieldType)
		}
		row = encoding.EncodeBytesValue(row, i, field.Value())
	}
	return
}

func decodeStoreField(fieldName string, row []byte) ([]document.Field, error) {
	if len(row) <= 1 {
		return nil, errors.New("invalid field row")
	}
	var fields []document.Field
	var err error
	fieldType := FIELD_TYPE(row[0])
	row = row[1:]
	for len(row) > 0 {
		var val []byte
		switch fieldType {
		case FIELD_TYPE_D:
			row, val, err = encoding.DecodeBytesValue(row)
			if err != nil {
				return nil, err
			}
			fields = append(fields, document.NewDateTimeFieldByBytes(fieldName, val))
		case FIELD_TYPE_B:
			row, val, err = encoding.DecodeBytesValue(row)
			if err != nil {
				return nil, err
			}
			fields = append(fields, document.NewBooleanFieldByBytes(fieldName, val))
		case FIELD_TYPE_N:
			row, val, err = encoding.DecodeBytesValue(row)
			if err != nil {
				return nil, err
			}
			fields = append(fields, document.NewNumericFieldFromBytes(fieldName, val))
		case FIELD_TYPE_T:
			row, val, err = encoding.DecodeBytesValue(row)
			if err != nil {
				return nil, err
			}
			fields = append(fields, document.NewTextField(fieldName, val))
		default:
			return nil, errors.New("invalid field type")
		}
	}
	return fields, nil
}
