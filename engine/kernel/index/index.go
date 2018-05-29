package index

type DocIndexIter func(docId DOC_ID, freq int) bool

type Index interface {
	GetDocument(docId DOC_ID) (*Document, error)
	GetDocIndex(filedId FIELD_ID, term []byte, iter DocIndexIter)
}
