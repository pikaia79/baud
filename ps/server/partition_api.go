package server

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/tiglabs/baudengine/common/content"
	"github.com/tiglabs/baudengine/common/keys"
	"github.com/tiglabs/baudengine/kernel/document"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/uuid"
)

func (p *partition) getInternal(request *pspb.GetRequest, response *pspb.GetResponse) {
	if err := p.validate(); err != nil {
		response.Error = *err
		if err.NotLeader != nil {
			response.Code = metapb.PS_RESP_CODE_NOT_LEADER
			response.Message = fmt.Sprintf("node[%d] of partition[%d] is not leader", p.server.nodeID, request.PartitionID)
		} else if err.NoLeader != nil {
			response.Code = metapb.PS_RESP_CODE_NO_LEADER
			response.Message = fmt.Sprintf("node[%d] of partition[%d] has no leader", p.server.nodeID, request.PartitionID)
		}

		log.Error("get document error, request is:[%v], \n error is:[%v]", request, err)
		return
	}

	var fields map[string]interface{}
	fields, response.Found = p.store.GetDocument(keys.EncodeDocID(&request.Id), request.StoredFields)
	if response.Found && len(fields) > 0 {
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

func (p *partition) bulkInternal(request *pspb.BulkRequest, response *pspb.BulkResponse) {
	if err := p.validate(); err != nil {
		response.Error = *err
		if err.NotLeader != nil {
			response.Code = metapb.PS_RESP_CODE_NOT_LEADER
			response.Message = fmt.Sprintf("node[%d] of partition[%d] is not leader", p.server.nodeID, request.PartitionID)
		} else if err.NoLeader != nil {
			response.Code = metapb.PS_RESP_CODE_NO_LEADER
			response.Message = fmt.Sprintf("node[%d] of partition[%d] has no leader", p.server.nodeID, request.PartitionID)
		}

		return
	}

	for _, itemReq := range request.Requests {
		itemResp := &pspb.BulkItemResponse{OpType: itemReq.OpType}
		switch itemReq.OpType {
		case pspb.OpType_INDEX:
			resp, err := p.indexInternal(request.ContentType, itemReq.Index)
			if err != nil {
				log.Error("index document error, request[%s] is:[%v], \n error is:[%v]", request.ReqId, itemReq.Index, err)
				itemResp.Failure = &pspb.Failure{Cause: err.Error()}
			} else {
				itemResp.Index = resp
			}
			response.Responses = append(response.Responses, *itemResp)

		case pspb.OpType_UPDATE:
			resp, err := p.updateInternal(request.ContentType, itemReq.Update)
			if err != nil {
				log.Error("update document error, request[%s] is:[%v], \n error is:[%v]", request.ReqId, itemReq.Update, err)
				itemResp.Failure = &pspb.Failure{Id: keys.EncodeDocIDToString(&itemReq.Update.Id), Cause: err.Error()}
			} else {
				itemResp.Update = resp
			}
			response.Responses = append(response.Responses, *itemResp)

		case pspb.OpType_DELETE:
			resp, err := p.deleteInternal(request.ContentType, itemReq.Delete)
			if err != nil {
				log.Error("delete document error, request[%s] is:[%v], \n error is:[%v]", request.ReqId, itemReq.Delete, err)
				itemResp.Failure = &pspb.Failure{Id: keys.EncodeDocIDToString(&(itemReq.Delete.Id)), Cause: err.Error()}
			} else {
				itemResp.Delete = resp
			}
			response.Responses = append(response.Responses, *itemResp)
		}
	}

	return
}

func (p *partition) indexInternal(contentType pspb.RequestContentType, request *pspb.IndexRequest) (*pspb.IndexResponse, error) {
	if err := p.validate(); err != nil {
		if err.NotLeader != nil {
			return nil, errors.New("is not leader")
		} else if err.NoLeader != nil {
			return nil, errors.New("has no leader")
		}
	}

	parser, _ := content.CreateParser(contentType, request.Source)
	val, err := parser.MapValues()
	parser.Close()
	if err != nil {
		return nil, err
	}

	seqNo, _ := strconv.ParseUint(uuid.TimeUUID(), 10, 64)
	docID := &metapb.DocID{SlotID: request.Slot, SeqNo: seqNo}
	doc := document.NewDocument(keys.EncodeDocID(docID))
	for n, v := range val {
		doc.AddField(document.NewTextField(n, []byte(v.(string)), document.StoreField))
	}

	if err := p.store.AddDocument(doc); err != nil {
		return nil, err
	}

	return &pspb.IndexResponse{Id: keys.EncodeDocIDToString(docID), Result: pspb.WriteResult_CREATED}, nil
}

func (p *partition) updateInternal(contentType pspb.RequestContentType, request *pspb.UpdateRequest) (*pspb.UpdateResponse, error) {
	if err := p.validate(); err != nil {
		if err.NotLeader != nil {
			return nil, errors.New("is not leader")
		} else if err.NoLeader != nil {
			return nil, errors.New("has no leader")
		}
	}

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

	found, err := p.store.UpdateDocument(doc, request.DocAsUpsert)
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

func (p *partition) deleteInternal(contentType pspb.RequestContentType, request *pspb.DeleteRequest) (*pspb.DeleteResponse, error) {
	if err := p.validate(); err != nil {
		if err.NotLeader != nil {
			return nil, errors.New("is not leader")
		} else if err.NoLeader != nil {
			return nil, errors.New("has no leader")
		}
	}

	n, err := p.store.DeleteDocument(keys.EncodeDocID(&request.Id))
	if err != nil {
		return nil, err
	}

	result := pspb.WriteResult_NOT_FOUND
	if n > 0 {
		result = pspb.WriteResult_DELETED
	}
	return &pspb.DeleteResponse{Id: keys.EncodeDocIDToString(&request.Id), Result: result}, nil
}
