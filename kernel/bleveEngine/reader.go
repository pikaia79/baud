package bleveEngine

import (
	"context"
	"github.com/tiglabs/baudengine/kernel"
)

func(r *BleveEngine)GetApplyID() (uint64, error) {

}

func(r *BleveEngine)GetDocument(ctx context.Context, docID kernel.DOC_ID) (kernel.DOCUMENT, bool) {

}

func(r *BleveEngine)Search(ctx context.Context, req *kernel.SearchRequest)(*kernel.SearchResult, error) {

}

func (r *BleveEngine)Close() error {

}