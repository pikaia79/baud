package router

import (
	"net/http"
	"github.com/julienschmidt/httprouter"
	"strconv"
	"github.com/juju/errors"
	"sync"
)

type Router struct {
	httpRouter *httprouter.Router
	httpServer *http.Server
	dbs        map[string]*DB
	lock     sync.RWMutex
}

func NewServer() *Router {
	return new(Router)
}

func (router *Router) Start(cfg *Config) error {
	router.dbs = make(map[string]*DB)
	router.httpRouter = httprouter.New()

	router.httpRouter.PUT("/:db/:space/:slot", router.handleCreate)
	router.httpRouter.GET("/:db/:space/:slot/:docId", router.handleRead)
	router.httpRouter.POST("/:db/:space/:slot/:docId", router.handleUpdate)
	router.httpRouter.DELETE("/:db/:space/:slot/:docId", router.handleDelete)

	router.httpRouter.GET("/status", router.handleStatus)
	router.httpRouter.GET("/debug/ppro", router.handlePprof)

	router.httpServer = &http.Server {
		Addr: cfg.Ip + ":" + strconv.Itoa(int(cfg.HttpPort)),
		Handler: router.httpRouter,
	}
	return router.httpServer.ListenAndServe()
}

func (router *Router) Shutdown() {
	router.httpServer.Close()
}

func (router *Router) handleCreate(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

	_, _, partition, _ := router.getParams(params, false)
	var docJson []byte
	_, err := request.Body.Read(docJson)
	if err != nil {
		panic(err)
	}
	key := partition.Create(docJson)
	writer.Write(key)
}

func (router *Router) handleRead(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

	_, _, partition, docId := router.getParams(params, true)
	var docJson []byte
	_, err := request.Body.Read(docJson)
	if err != nil {
		panic(err)
	}
	key := partition.Read(docId)
	writer.Write(key)
}

func (router *Router) handleUpdate(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

	_, _, partition, docId := router.getParams(params, true)
	var docJson []byte
	_, err := request.Body.Read(docJson)
	if err != nil {
		panic(err)
	}
	partition.Update(docId, docJson)
}

func (router *Router) handleDelete(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

	_, _, partition, docId := router.getParams(params, true)
	partition.Delete(docId)
}

func (router *Router) handleStatus(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

}

func (router *Router) handlePprof(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	defer router.catchPanic(writer)

}

func (router *Router) getParams(params httprouter.Params, decodeDocId bool) (db *DB, space *Space, partition *Partition, docId uint64) {
	db = router.getDB(params.ByName("db"))
	space = db.GetSpace(params.ByName("space"))
	slot, err := strconv.ParseUint(params.ByName("slot"), 10, 32)
	if err != nil {
		panic(err)
	}
	partition = space.GetPartition(uint32(slot))
	if !decodeDocId {
		return
	}
	docId, err = strconv.ParseUint(params.ByName("docId"), 10, 64)
	if err != nil {
		panic(err)
	}
	return
}

func (router *Router) getDB(dbName string) *DB {
	router.lock.RLock()
	defer router.lock.RUnlock()

	db, ok := router.dbs[dbName]
	if !ok {
		//todo: get db info from master
		panic(errors.New("cannot get db!"))
	}
	return db
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

