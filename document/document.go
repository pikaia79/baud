package document

import (
	"fmt"
)

type Document struct {
	ID       string        `json:"_id"`
	SlotID   uint32        `json:"_slot"`
	Fields   []Field       `json:"fields"`
	Version  uint64        `json:"_version"`
	Source   []byte        `json:"_source"`
}

func NewDocument(id string, slotID uint32, version uint64, source []byte) *Document {
	return &Document{
		ID:              id,
		SlotID:          slotID,
		Version:         version,
		Fields:          make([]Field, 0),
		Source:          source,
	}
}

func (d *Document) AddField(f Field) *Document {
	d.Fields = append(d.Fields, f)
	return d
}

func (d *Document) String() string {
	fields := ""
	for i, field := range d.Fields {
		if i != 0 {
			fields += ", "
		}
		fields += fmt.Sprintf("%#v", field)
	}
	return fmt.Sprintf("&document.Document{ID:%s, Fields: %s, Source: %s}", d.ID, fields, string(d.Source))
}
