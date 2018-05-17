package router

import (
	"encoding/json"
	"github.com/spaolacci/murmur3"
	"github.com/tiglabs/baudengine/common/keys"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util/log"
	"net/http"
	"strconv"
	"sync"
	"github.com/tiglabs/baudengine/util/netutil"
	"errors"
)

var routerCfg 	*Config

type Router struct {
	httpServer   *netutil.Server
	masterClient *MasterClient
	dbMap        sync.Map
	lock         sync.RWMutex
}

type HttpReply struct {
	Code int32       `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data,omitempty"`
}

func NewServer() *Router {
	return new(Router)
}

func (router *Router) Start(cfg *Config) error {
	routerCfg = cfg
	router.masterClient = NewMasterClient(cfg.ModuleCfg.MasterAddr)

	httpServerConfig := &netutil.ServerConfig{
		Name: "router",
		Addr: cfg.ModuleCfg.Ip + ":" + strconv.Itoa(int(cfg.ModuleCfg.HttpPort)),
		ConnLimit: 10000,
	}
	router.httpServer = netutil.NewServer(httpServerConfig)

	router.httpServer.Handle(netutil.PUT, "/doc/:db/:space", router.handleCreate)
	router.httpServer.Handle(netutil.GET, "/doc/:db/:space/:docId", router.handleRead)
	router.httpServer.Handle(netutil.POST,"/doc/:db/:space/:docId", router.handleUpdate)
	router.httpServer.Handle(netutil.DELETE, "/doc/:db/:space/:docId", router.handleDelete)

	return router.httpServer.Run()
}

func (router *Router) Shutdown() {
	router.httpServer.Close()
}

func (router *Router) handleCreate(writer http.ResponseWriter, request *http.Request, params netutil.UriParams) {
	defer router.catchPanic(writer)

	db, space, _, _ := router.getParams(params, false)
	docBody := router.readDocBody(request)
	var partition *Partition
	keyField := space.GetKeyField()
	if keyField != "" {
		docObj := make(map[string]interface{})
		if err := json.Unmarshal(docBody, &docObj); err != nil {
			panic(err)
		}
		if slotData, ok := docObj[keyField]; ok {
			h32 := murmur3.New32()
			switch data := slotData.(type) {
			case string:
				h32.Write([]byte(data))
			default:
				panic(errors.New("bad slot data type"))
			}
			partition = space.GetPartition(metapb.SlotID(h32.Sum32()))
		}
	}
	if partition == nil {
		h32 := murmur3.New32()
		h32.Write(docBody)
		partition = space.GetPartition(metapb.SlotID(h32.Sum32()))
	}
	docId := partition.Create(docBody)

	respMap := map[string]interface{}{
		"_db":    db.meta.ID,
		"_space": space.meta.ID,
		"_slot":  params.ByName("slot"),
		"_docId": docId,
	}

	sendReply(writer, &HttpReply{ERRCODE_SUCCESS, ErrSuccess.Error(), respMap})
}

func (router *Router) handleRead(writer http.ResponseWriter, request *http.Request, params netutil.UriParams) {
	defer router.catchPanic(writer)

	_, _, partition, docId := router.getParams(params, true)
	docBody := partition.Read(docId)
	sendReply(writer, &HttpReply{ERRCODE_SUCCESS, ErrSuccess.Error(), docBody})
}

func (router *Router) handleUpdate(writer http.ResponseWriter, request *http.Request, params netutil.UriParams) {
	defer router.catchPanic(writer)

	_, _, partition, docId := router.getParams(params, true)
	docBody := router.readDocBody(request)
	partition.Update(docId, docBody)
	sendReply(writer, &HttpReply{ERRCODE_SUCCESS, ErrSuccess.Error(), nil})
}

func (router *Router) handleDelete(writer http.ResponseWriter, request *http.Request, params netutil.UriParams) {
	defer router.catchPanic(writer)

	_, _, partition, docId := router.getParams(params, true)
	if ok := partition.Delete(docId); ok {
		sendReply(writer, &HttpReply{ERRCODE_SUCCESS, ErrSuccess.Error(), nil})
	} else {
		sendReply(writer, &HttpReply{ERRCODE_INTERNAL_ERROR, "Cannot delete doc", nil})
	}
}

func (router *Router) getParams(params netutil.UriParams, decodeDocId bool) (db *DB, space *Space, partition *Partition, docId *metapb.DocID) {
	defer func() {
		if p := recover(); p != nil {
			if err, ok := p.(error); ok {
				log.Error("getParams() failed: %s", err.Error())
			}
			panic(&HttpReply{ERRCODE_PARAM_ERROR, ErrParamError.Error(), nil})
		}
	}()

	db = router.GetDB(params.ByName("db"))
	space = db.GetSpace(params.ByName("space"))
	if decodeDocId {
		id, err := keys.DecodeDocIDFromString(params.ByName("docId"))
		if err != nil {
			panic(err)
		}
		docId = id
	}
	return
}

func (router *Router) readDocBody(request *http.Request) []byte {
	var docBody = make([]byte, request.ContentLength)
	request.Body.Read(docBody)
	return docBody
}

func (router *Router) GetDB(dbName string) *DB {
	db, ok := router.dbMap.Load(dbName)
	if !ok {
		db, ok = router.dbMap.LoadOrStore(dbName, NewDB(router.masterClient, router.masterClient.GetDB(dbName)))
	}
	return db.(*DB)
}

func (router *Router) catchPanic(writer http.ResponseWriter) {
	if p := recover(); p != nil {
		switch t := p.(type) {
		case *HttpReply:
			sendReply(writer, t)
		case error:
			sendReply(writer, &HttpReply{ERRCODE_INTERNAL_ERROR, t.Error(), nil})
			log.Error("catchPanic() error: %s", t.Error())
		default:
			sendReply(writer, &HttpReply{ERRCODE_INTERNAL_ERROR, ErrInternalError.Error(), nil})
		}
	}
}

func sendReply(writer http.ResponseWriter, httpReply *HttpReply) {
	writer.WriteHeader(200)
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.Error("fail to marshal http reply[%v]. err:[%v]", httpReply, err)
		reply, _ = json.Marshal(HttpReply{ERRCODE_INTERNAL_ERROR, "json.Marshal() failed", nil})
	}
	writer.Header().Set("content-type", "application/json")
	writer.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := writer.Write(reply); err != nil {
		log.Error("fail to write http reply[%s] len[%d]. err:[%v]", string(reply), len(reply), err)
	}
}
