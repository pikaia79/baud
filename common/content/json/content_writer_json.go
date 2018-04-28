package json

import (
	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/util/json"
)

type contentWriterJSON struct {
	data []byte
}

func (w *contentWriterJSON) Bytes() []byte {
	return w.data
}

func (w *contentWriterJSON) ContentType() pspb.RequestContentType {
	return pspb.RequestContentType_JSON
}

func (w *contentWriterJSON) WriteMap(v map[string]interface{}) error {
	b, err := json.Marshal(&v)
	if err == nil {
		w.data = b
	}

	return err
}

func (w *contentWriterJSON) Close() error {
	w.data = nil
	writerPool.Put(w)

	return nil
}
