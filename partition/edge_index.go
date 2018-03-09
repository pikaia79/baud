package partition

import (
	. "github.com/tiglabs/baud/types"
)

type EdgeIndex struct{}

func (i *EdgeIndex) Add(e Edge) error {
	return nil
}

func (i *EdgeIndex) Del(e Edge) error {
	return nil
}

func (i *EdgeIndex) Search(req *Request) (res *Result, e erro) {
	return
}
