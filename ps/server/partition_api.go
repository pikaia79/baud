package server

import (
	"context"
	"fmt"

	"time"

	"github.com/tiglabs/baudengine/common/content"
	"github.com/tiglabs/baudengine/common/keys"
	"github.com/tiglabs/baudengine/kernel/document"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/proto/pspb/raftpb"
	"github.com/tiglabs/baudengine/util/log"
)

func (p *partition) getInternal(request *pspb.GetRequest, response *pspb.GetResponse) {
	if err := p.checkReadAble(true); err != nil {
		response.Error = *err
		if err.NotLeader != nil {
			response.Code = metapb.PS_RESP_CODE_NOT_LEADER
			response.Message = fmt.Sprintf("node[%d] of partition[%d] is not leader", p.server.nodeID, request.PartitionID)
		} else if err.NoLeader != nil {
			response.Code = metapb.PS_RESP_CODE_NO_LEADER
			response.Message = fmt.Sprintf("node[%d] of partition[%d] has no leader", p.server.nodeID, request.PartitionID)
		} else if err.PartitionNotFound != nil {
			response.Code = metapb.PS_RESP_CODE_NO_PARTITION
			response.Message = fmt.Sprintf("node[%d] of partition[%d] has closed", p.server.nodeID, request.PartitionID)
		}

		log.Error("get document error, request is:[%v], \n error is:[%v]", request, response.Message)
		return
	}

	var (
		err     error
		fields  map[string]interface{}
		cancel  context.CancelFunc
		timeCtx = p.ctx
	)
	if request.Timeout != "" {
		if timeout, err := time.ParseDuration(request.Timeout); err == nil {
			timeCtx, cancel = context.WithTimeout(timeCtx, timeout)
		}
	}
	fields, response.Found = p.store.GetDocument(timeCtx, keys.EncodeDocID(&request.Id), request.StoredFields)
	select {
	case <-timeCtx.Done():
		err = timeCtx.Err()
	default:
		if cancel != nil {
			cancel()
		}
	}

	if err != nil {
		if err == context.DeadlineExceeded {
			response.Code = metapb.RESP_CODE_TIMEOUT
			response.Message = "request timeout"
		} else {
			response.Code = metapb.RESP_CODE_SERVER_ERROR
			response.Message = "server stopped"
		}
		log.Error("get document error, request is:[%v], \n error is:[%v]", request, err)
	} else if response.Found && len(fields) > 0 {
		contentWr, _ := content.CreateWriter(request.ContentType)
		if err := contentWr.WriteMap(fields); err == nil {
			response.Fields = contentWr.Bytes()
		} else {
			response.Code = metapb.RESP_CODE_SERVER_ERROR
			response.Message = err.Error()
			log.Error("marshal document fileds error, request is:[%v], \n error is:[%v]", request, err)
		}
		contentWr.Close()
	}

	return
}

func (p *partition) execWriteCommand(index uint64, cmd *raftpb.WriteCommand) (resp interface{}, err error) {
	select {
	case <-p.ctx.Done():
		err = errorPartitonClosed
		return

	default:
	}

	switch cmd.OpType {
	case pspb.OpType_INDEX:
		if resp, err = p.indexInternal(cmd.ContentType, index, cmd.Index); err != nil {
			log.Error("index document error, request is:[%v], \n error is:[%v]", cmd.Index, err)
		}

	case pspb.OpType_UPDATE:
		if resp, err = p.updateInternal(cmd.ContentType, index, cmd.Update); err != nil {
			log.Error("update document error, request is:[%v], \n error is:[%v]", cmd.Update, err)
		}

	case pspb.OpType_DELETE:
		if resp, err = p.deleteInternal(cmd.ContentType, index, cmd.Delete); err != nil {
			log.Error("delete document error, request is:[%v], \n error is:[%v]", cmd.Delete, err)
		}

	default:
		p.store.SetApplyID(index)
		err = errorPartitonCommand
		log.Error("unsupported request operation[%s]", cmd.OpType)
	}

	return
}

func (p *partition) indexInternal(contentType pspb.RequestContentType, index uint64, request *pspb.IndexRequest) (*pspb.IndexResponse, error) {
	parser, _ := content.CreateParser(contentType, request.Source)
	val, err := parser.MapValues()
	parser.Close()
	if err != nil {
		return nil, err
	}

	docID := &metapb.DocID{SlotID: request.Slot, SeqNo: index}
	doc := document.NewDocument(keys.EncodeDocID(docID))
	for n, v := range val {
		doc.AddField(document.NewTextField(n, []byte(v.(string)), document.StoreField))
	}

	if err := p.store.AddDocument(p.ctx, doc, index); err != nil {
		return nil, err
	}
	return &pspb.IndexResponse{Id: keys.EncodeDocIDToString(docID), Result: pspb.WriteResult_CREATED}, nil
}

func (p *partition) updateInternal(contentType pspb.RequestContentType, index uint64, request *pspb.UpdateRequest) (*pspb.UpdateResponse, error) {
	parser, _ := content.CreateParser(contentType, request.Doc)
	val, err := parser.MapValues()
	parser.Close()
	if err != nil {
		return nil, err
	}

	doc := document.NewDocument(keys.EncodeDocID(&request.Id))
	for n, v := range val {
		doc.AddField(document.NewTextField(n, []byte(v.(string)), document.StoreField))
	}

	found, err := p.store.UpdateDocument(p.ctx, doc, request.DocAsUpsert, index)
	if err != nil {
		return nil, err
	}

	result := pspb.WriteResult_NOT_FOUND
	if found {
		result = pspb.WriteResult_UPDATED
	} else if request.DocAsUpsert {
		result = pspb.WriteResult_CREATED
	}
	return &pspb.UpdateResponse{Id: keys.EncodeDocIDToString(&request.Id), Result: result}, nil
}

func (p *partition) deleteInternal(contentType pspb.RequestContentType, index uint64, request *pspb.DeleteRequest) (*pspb.DeleteResponse, error) {
	n, err := p.store.DeleteDocument(p.ctx, keys.EncodeDocID(&request.Id), index)
	if err != nil {
		return nil, err
	}

	result := pspb.WriteResult_NOT_FOUND
	if n > 0 {
		result = pspb.WriteResult_DELETED
	}
	return &pspb.DeleteResponse{Id: keys.EncodeDocIDToString(&request.Id), Result: result}, nil
}
