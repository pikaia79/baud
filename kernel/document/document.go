package document

type Document struct {
	ID              string  `json:"id"`
	// _version, _source as special field for document
	Fields          []Field `json:"fields"`
}

func NewDocument(id string) *Document {
	return &Document{
		ID:              id,
		Fields:          make([]Field, 0),
	}
}

func (d *Document) AddField(f Field) *Document {
	d.Fields = append(d.Fields, f)
	return d
}

func (d *Document) FindField(name string) Field {
	for _, f := range d.Fields {
		if f.Name() == name {
			return f
		}
	}
	return nil
}
