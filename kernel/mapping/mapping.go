package mapping

import "github.com/tiglabs/baud/document"

// TODO
type Mapper struct {

}

func (m *Mapper) GetFieldID(name string) (uint64, bool) {
	return 0, true
}

func (m *Mapper) GetFieldName(id uint64) (string, bool) {
	return "", true
}

func (m *Mapper) GetFieldTypeByID(id uint64) (document.FieldType, bool) {
	return document.String, true
}

func (m *Mapper) GetFieldTypeByName(name string) (document.FieldType, bool) {
	return document.String, true
}
