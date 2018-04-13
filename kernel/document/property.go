package document

type Property int

const (
	//
	IndexField Property = 1 << iota
	StoreField
	TermVectors
	// doc_id -> term values
	DocValues
)

func (p Property) IsIndexed() bool {
	return p&IndexField != 0
}

func (p Property) IsStored() bool {
	return p&StoreField != 0
}

func (p Property) IncludeTermVectors() bool {
	return p&TermVectors != 0
}

func (p Property) IncludeDocValues() bool {
	return p&DocValues != 0
}

func (p Property) String() string {
	rv := ""
	if p.IsIndexed() {
		rv += "Indexed"
	}
	if p.IsStored() {
		if rv != "" {
			rv += "|"
		}
		rv += "Store"
	}
	if p.IncludeTermVectors() {
		if rv != "" {
			rv += "|"
		}
		rv += "TermVectors"
	}
	if p.IncludeDocValues() {
		if rv != "" {
			rv += "|"
		}
		rv += "DocValues"
	}
	return rv
}
