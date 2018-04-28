package json

import (
	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/util/json"
)

// contentParserJSON parser for json content type
type contentParserJSON struct {
	data []byte
}

func (p *contentParserJSON) ContentType() pspb.RequestContentType {
	return pspb.RequestContentType_JSON
}

func (p *contentParserJSON) MapValues() (map[string]interface{}, error) {
	v := make(map[string]interface{})
	err := json.Unmarshal(p.data, &v)
	return v, err
}

func (p *contentParserJSON) Close() error {
	p.data = nil
	parserPool.Put(p)
	return nil
}
