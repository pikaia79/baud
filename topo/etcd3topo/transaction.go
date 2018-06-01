package etcd3topo

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/tiglabs/baudengine/topo"
	"path"
	"reflect"
)

type etcd3Transaction struct {
	client  *cellClient
	txn     clientv3.Txn
	cmps    []clientv3.Cmp
	thenOps []clientv3.Op
	elseOps []clientv3.Op
}

func (t *etcd3Transaction) Create(filePath string, contents []byte) {
	nodePath := path.Join(t.client.root, filePath)
	t.cmps = append(t.cmps, clientv3.Compare(clientv3.Version(nodePath), "=", 0))
	t.thenOps = append(t.thenOps, clientv3.OpPut(nodePath, string(contents)))

}

func (t *etcd3Transaction) Delete(filePath string, version topo.Version) {
	// TODO: force to delete it when version is nil
	nodePath := path.Join(t.client.root, filePath)
	t.cmps = append(t.cmps, clientv3.Compare(clientv3.Version(nodePath), "=", int64(version.(EtcdVersion))))
	t.thenOps = append(t.thenOps, clientv3.OpDelete(nodePath))
	t.elseOps = append(t.elseOps, clientv3.OpGet(nodePath))
}

func (t *etcd3Transaction) Commit() ([]topo.TxnOpResult, error) {
	if len(t.cmps) != 0 {
		t.txn = t.txn.If(t.cmps...)
	}
	if len(t.thenOps) != 0 {
		t.txn = t.txn.Then(t.thenOps...)
	}
	if len(t.elseOps) != 0 {
		t.txn = t.txn.Else(t.elseOps...)
	}

	txnResp, err := t.txn.Commit()
	if err != nil {
		return nil, convertError(err)
	}
	if !txnResp.Succeeded {
		return nil, topo.ErrNodeExists
	}

	opResps := txnResp.Responses
	if opResps == nil || len(opResps) == 0 {
		return nil, nil
	}

	opResults := make([]topo.TxnOpResult, 0)
	for _, opResp := range opResps {
		var opResult topo.TxnOpResult

		switch opResp.Response.(type) {
		case *etcdserverpb.ResponseOp_ResponsePut:
			newVer := EtcdVersion(opResp.GetResponsePut().Header.Revision)
			opResult = &topo.TxnCreateOpResult{Typ: topo.OPTYPE_CREATE, Version: newVer}
		case *etcdserverpb.ResponseOp_ResponseDeleteRange:
			// TODO:
		case *etcdserverpb.ResponseOp_ResponseRange:
			// TODO:
		default:
			opResult = &topo.TxnErrorOpResult{Typ: topo.OPTYPE_ERROR,
				Err: errors.New(fmt.Sprintf("invalid op resp type[%v] for etcd3 transaction",
					reflect.TypeOf(opResp.Response).Name()))}
		}

		opResults = append(opResults, opResult)
	}

	return opResults, nil
}

func (s *Server) NewTransaction(ctx context.Context, cell string) (topo.Transaction, error) {
	c, err := s.clientForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	txn := &etcd3Transaction{
		client:  c,
		txn:     c.cli.Txn(ctx),
		cmps:    make([]clientv3.Cmp, 0),
		thenOps: make([]clientv3.Op, 0),
		elseOps: make([]clientv3.Op, 0),
	}
	return txn, nil
}
