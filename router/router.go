package router

import (
	"encoding/json"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
	"github.com/tiglabs/baudengine/keys"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/raft/util/log"
	"net/http"
	"strconv"
	"sync"
)

type Router struct {
	httpRouter   *httprouter.Router
	httpServer   *http.Server
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
	router.masterClient = NewMasterClient(cfg.MasterAddr)
	router.httpRouter = httprouter.New()

	router.httpRouter.PUT("/doc/:db/:space", router.handleCreate)
	router.httpRouter.GET("/doc/:db/:space/:docId", router.handleRead)
	router.httpRouter.POST("/doc/:db/:space/:docId", router.handleUpdate)
	router.httpRouter.DELETE("/doc/:db/:space/:docId", router.handleDelete)

	router.httpRouter.GET("/status", router.handleStatus)
	router.httpRouter.GET("/debug/ppro", router.handlePprof)

	router.httpServer = &http.Server{
		Addr:    cfg.Ip + ":" + strconv.Itoa(int(cfg.HttpPort)),
		Handler: router.httpRouter,
	}
	return router.httpServer.ListenAndServe()
}

func (router *Router) Shutdown() {
	router.httpServer.Close()
}

func (router *Router) handleCreate(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

	db, space, _, _ := router.getParams(params, true)
	docBody := router.readDocBody(request)
	var partition *Partition
	keyField := space.GetKeyField()
	if keyField != "" {
		var docObj map[string]string
		if err := json.Unmarshal(docBody, docObj); err != nil {
			panic(err)
		}
		if slotData, ok := docObj[keyField]; ok {
			h32 := murmur3.New32()
			h32.Write([]byte(slotData))
			partition = space.GetPartition(h32.Sum32())
		} else {
			panic(errors.New("cannot get slot data"))
		}
	} else {
		h32 := murmur3.New32()
		h32.Write(docBody)
		partition = space.GetPartition(h32.Sum32())
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

func (router *Router) handleRead(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

	_, _, partition, docId := router.getParams(params, false)
	docBody := partition.Read(docId)
	sendReply(writer, &HttpReply{ERRCODE_SUCCESS, ErrSuccess.Error(), docBody})
}

func (router *Router) handleUpdate(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

	_, _, partition, docId := router.getParams(params, false)
	docBody := router.readDocBody(request)
	partition.Update(docId, docBody)
	sendReply(writer, &HttpReply{ERRCODE_SUCCESS, ErrSuccess.Error(), nil})
}

func (router *Router) handleDelete(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

	_, _, partition, docId := router.getParams(params, false)
	if ok := partition.Delete(docId); ok {
		sendReply(writer, &HttpReply{ERRCODE_SUCCESS, ErrSuccess.Error(), nil})
	} else {
		sendReply(writer, &HttpReply{ERRCODE_INTERNAL_ERROR, "Cannot delete doc", nil})
	}
}

func (router *Router) handleStatus(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

	panic(errors.New("test"))
}

func (router *Router) handlePprof(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

}

func (router *Router) getParams(params httprouter.Params, decodeSlot bool) (db *DB, space *Space, partition *Partition, docId *metapb.DocID) {
	defer func() {
		if p := recover(); p != nil {
			panic(&HttpReply{ERRCODE_PARAM_ERROR, ErrParamError.Error(), nil})
		}
	}()

	db = router.GetDB(params.ByName("db"))
	space = db.GetSpace(params.ByName("space"))
	if decodeSlot {
		slot, err := strconv.ParseUint(params.ByName("slot"), 10, 32)
		if err != nil {
			panic(err)
		}
		partition = space.GetPartition(uint32(slot))
	} else {
		id, err := keys.DecodeDocIDFromString(params.ByName("docId"))
		if err != nil {
			panic(err)
		}
		docId = id
	}
	return
}

func (router *Router) readDocBody(request *http.Request) []byte {
	var docBody []byte
	_, err := request.Body.Read(docBody)
	if err != nil {
		panic(err)
	}
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
