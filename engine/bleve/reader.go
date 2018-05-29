package bleve

import (
	"context"
	"errors"
	"fmt"
	"encoding/binary"

	"github.com/blevesearch/bleve/document"
	"github.com/tiglabs/baudengine/engine"
)

func(r *BleveEngine)GetApplyID() (uint64, error) {
	v, err := r.driver.GetInternal(RAFT_APPLY_ID)
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}
	if len(v) != 8 {
		return 0, errors.New("invalid applyID value in store")
	}
	return binary.BigEndian.Uint64(v), nil
}

func(r *BleveEngine)GetDocument(ctx context.Context, docID engine.DOC_ID) (engine.DOCUMENT, bool) {
	_doc, err := r.driver.Document(docID.ToString())
	if err != nil {
		// todo panic ???
		return nil, false
	}
	doc := make(engine.DOCUMENT)
	for _, field := range _doc.Fields {
		switch f := field.(type) {
		case *document.TextField:
			doc[f.Name()] = string(f.Value())
		case *document.BooleanField:
			b, err := f.Boolean()
			if err != nil {
				// TODO panic ??
				return nil, false
			}
			doc[f.Name()] = b
		case *document.DateTimeField:
			t, err := f.DateTime()
			if err != nil {
				return nil, false
			}
			doc[f.Name()] = t
		case *document.GeoPointField:
			lat, err := f.Lat()
			if err != nil {
				return nil, false
			}
			lon, err := f.Lon()
			if err != nil {
				return nil, false
			}
			doc[f.Name()] = fmt.Sprintf("%f, %f", lat, lon)
		case *document.NumericField:
			num, err := f.Number()
			if err != nil {
				return nil, false
			}
			doc[f.Name()] = num
		default:
			// todo panic ??
			return nil, false
		}
	}
	return doc, true
}

func(r *BleveEngine)Search(ctx context.Context, req *engine.SearchRequest)(*engine.SearchResult, error) {

}

func (r *BleveEngine)Close() error {
	return r.driver.Close()
}