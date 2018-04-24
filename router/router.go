package router

import (
	"encoding/json"
	"github.com/julienschmidt/httprouter"
	"github.com/tiglabs/baud/keys"
	"github.com/tiglabs/baud/proto/metapb"
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

func NewServer() *Router {
	return new(Router)
}

func (router *Router) Start(cfg *Config) error {
	router.masterClient = NewMasterClient(cfg.MasterAddr)
	router.httpRouter = httprouter.New()

	router.httpRouter.PUT("/:db/:space/:slot", router.handleCreate)
	router.httpRouter.GET("/:db/:space/:docId", router.handleRead)
	router.httpRouter.POST("/:db/:space/:docId", router.handleUpdate)
	router.httpRouter.DELETE("/:db/:space/:docId", router.handleDelete)

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

	db, space, partition, _ := router.getParams(params, true)
	docBody := router.readDocBody(request)
	docId := partition.Create(docBody)

	respMap := map[string]interface{}{
		"_db":    db.meta.ID,
		"_space": space.meta.ID,
		"_slot":  params.ByName("slot"),
		"_docId": docId,
	}

	if data, err := json.Marshal(respMap); err == nil {
		writer.WriteHeader(200)
		writer.Write(data)
	}
}

func (router *Router) handleRead(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

	_, _, partition, docId := router.getParams(params, false)
	docBody := partition.Read(docId)
	writer.WriteHeader(200)
	writer.Write(docBody)
}

func (router *Router) handleUpdate(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

	_, _, partition, docId := router.getParams(params, false)
	docBody := router.readDocBody(request)
	partition.Update(docId, docBody)
	writer.WriteHeader(200)
	writer.Write([]byte("{status: 0, message: \"update success\"}"))
}

func (router *Router) handleDelete(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

	_, _, partition, docId := router.getParams(params, false)
	if ok := partition.Delete(docId); ok {
		writer.WriteHeader(200)
		writer.Write([]byte("{status: 0, message: \"delete success\"}"))
	} else {
		writer.WriteHeader(400)
		writer.Write([]byte("{status: -1, message: \"delete failed\"}"))
	}
}

func (router *Router) handleStatus(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

}

func (router *Router) handlePprof(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

}

func (router *Router) getParams(params httprouter.Params, decodeSlot bool) (db *DB, space *Space, partition *Partition, docId *metapb.DocID) {
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
		writer.WriteHeader(400)
		if err, ok := p.(error); ok {
			writer.Write([]byte(err.Error()))
		} else {
			writer.Write([]byte("unknown error"))
		}
	}
}
