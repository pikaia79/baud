package server

import (
	"fmt"

	"errors"

	"github.com/tiglabs/baudengine/common/content"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/util/log"
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

		log.Error("get document error, request is:[%v], error is:[%v]", request, err)
		return
	}

	var fields map[string]interface{}
	fields, response.Found = p.store.GetDocument(&request.Id, request.StoredFields)
	if response.Found && len(fields) > 0 {
		contentWr, _ := content.CreateWriter(request.ContentType)
		if err := contentWr.WriteMap(fields); err == nil {
			response.Fields = contentWr.Bytes()
		} else {
			response.Code = metapb.RESP_CODE_SERVER_ERROR
			response.Message = fmt.Sprintf("marshal document fileds error, request is:[%v], error is:[%v]", request, err)
		}
		contentWr.Close()
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

}
