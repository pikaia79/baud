package server

import (
	"fmt"

	"github.com/tiglabs/baud/proto/metapb"
	"github.com/tiglabs/baud/proto/pspb"
	"github.com/tiglabs/baud/util/json"
	"github.com/tiglabs/baud/util/log"
)

func (p *partition) internalGet(request *pspb.GetRequest, response *pspb.GetResponse) {
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
		if b, err := json.Marshal(fields); err == nil {
			response.Fields = b
		} else {
			response.Code = metapb.RESP_CODE_SERVER_ERROR
			response.Message = fmt.Sprintf("json marshal document fileds error, request is:[%v], error is:[%v]", request, err)
		}
	}

	return
}
