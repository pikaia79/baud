package document

type Document struct {
	ID              string  `json:"id"`
	// _version, _source, _all as special field for document
	Fields          map[string][]Field `json:"fields"`
}

func NewDocument(id string) *Document {
	return &Document{
		ID:              id,
	}
}

func (d *Document) AddField(f Field) *Document {
	if d.Fields == nil {
		d.Fields = make(map[string][]Field)
	}
	files := d.Fields[f.Name()]
	d.Fields[f.Name()] = append(files, f)
	return d
}

func (d *Document) FindFields(name string) []Field {
	if d.Fields == nil {
		return nil
	}
	return d.Fields[name]
}
