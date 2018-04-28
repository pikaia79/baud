package json

import "sync"

var parserPool = &sync.Pool{
	New: func() interface{} {
		return new(contentParserJSON)
	},
}

var writerPool = &sync.Pool{
	New: func() interface{} {
		return new(contentWriterJSON)
	},
}

// CreateParser create a contentParserJSON object
func CreateParser(data []byte) *contentParserJSON {
	p := parserPool.Get().(*contentParserJSON)
	p.data = data
	return p
}

// CreateWriter create a contentWriterJSON object
func CreateWriter() *contentWriterJSON {
	return writerPool.Get().(*contentWriterJSON)
}
