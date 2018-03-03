package object

type Index struct{}

func (i *Index) Index(o *Object) error {
	return nil
}

func (i *Index) Delete(oid OID) error {
	return nil
}

func (i *Index) Object(oid OID) (*Object, error) {
	return
}

func (i *Index) Search(*Request) (*Result, error) {
	return
}
