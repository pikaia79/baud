package ps

type ObjectIndex struct{}

func (i *ObjectIndex) Index(o *Object) error {
	return nil
}

func (i *ObjectIndex) Delete(oid OID) error {
	return nil
}

func (i *ObjectIndex) Object(oid OID) (*Object, error) {
	return
}

func (i *ObjectIndex) Search(*Request) (*Result, error) {
	return
}
