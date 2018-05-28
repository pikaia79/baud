package index

type DOC_ID []byte
type FIELD_ID uint32
type FIELD_VALUE []byte
type FIELD_TYPE int
type TERM []byte


func (docId DOC_ID) ToBytes() []byte {
	return []byte(docId)
}

func (fieldId FIELD_ID) ToInt() uint32 {
	return uint32(fieldId)
}

func (term TERM) ToString() string {
	return string(term)
}

type Field struct {
	FieldId   FIELD_ID
	FieldType FIELD_TYPE
	FieldValue FIELD_VALUE
}

type Document struct {
	DocId      DOC_ID
	Fields     []Field
}
