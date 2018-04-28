package content

import (
	"fmt"
	"io"

	"github.com/tiglabs/baudengine/common/content/json"
	"github.com/tiglabs/baudengine/proto/pspb"
)

// Parser parser for specific content type
type Parser interface {
	io.Closer
	ContentType() pspb.RequestContentType
	MapValues() (map[string]interface{}, error)
}

// Writer writer for specific content type
type Writer interface {
	io.Closer
	Bytes() []byte
	ContentType() pspb.RequestContentType
	WriteMap(map[string]interface{}) error
}

// CreateParser create a parser based on a particular content type
func CreateParser(contentType pspb.RequestContentType, data []byte) (Parser, error) {
	switch contentType {
	case pspb.RequestContentType_JSON:
		return json.CreateParser(data), nil
	default:
		return nil, fmt.Errorf("Unsupport ContentType %v", contentType)
	}
}

// CreateWriter create a writer based on a particular content type
func CreateWriter(contentType pspb.RequestContentType) (Writer, error) {
	switch contentType {
	case pspb.RequestContentType_JSON:
		return json.CreateWriter(), nil
	default:
		return nil, fmt.Errorf("Unsupport ContentType %v", contentType)
	}
}
