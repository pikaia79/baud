package rpc

import (
	"io"
	"sync"

	"github.com/golang/snappy"
)

var snappyWriterPool sync.Pool
var snappyReaderPool sync.Pool

type snappyWriter struct {
	*snappy.Writer
}

func (w *snappyWriter) Close() error {
	defer snappyWriterPool.Put(w)
	return w.Writer.Close()
}

type snappyReader struct {
	*snappy.Reader
}

func (r *snappyReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	if err == io.EOF {
		snappyReaderPool.Put(r)
	}
	return n, err
}

type snappyCompressor struct {
}

func (snappyCompressor) Name() string {
	return "snappy"
}

func (snappyCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	sw, ok := snappyWriterPool.Get().(*snappyWriter)
	if !ok {
		sw = &snappyWriter{snappy.NewBufferedWriter(w)}
	} else {
		sw.Reset(w)
	}
	return sw, nil
}

func (snappyCompressor) Decompress(r io.Reader) (io.Reader, error) {
	sr, ok := snappyReaderPool.Get().(*snappyReader)
	if !ok {
		sr = &snappyReader{snappy.NewReader(r)}
	} else {
		sr.Reset(r)
	}
	return sr, nil
}
