package netutil

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"sync/atomic"

	"golang.org/x/net/netutil"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/julienschmidt/httprouter"
	"github.com/tiglabs/baudengine/util/gogc"
)

type ServerConfig struct {
	Name      string
	Addr      string   // ip:port
	Version   string
	ConnLimit int
}

// Server is a http server
type Server struct {
	cfg 	  *ServerConfig
	server    *http.Server
	router    *httprouter.Router
	closed    int64
}

// NewServer creates the server with given configuration.
func NewServer(config *ServerConfig) *Server {
	s := &Server{
		cfg:    config,
		router: httprouter.New(),
	}

	s.Handle(GET, "/debug/ping", PingPong)
	s.Handle(GET, "/debug/pprof", DebugPprofHandler)
	s.Handle(GET, "/debug/pprof/cmdline", DebugPprofCmdlineHandler)
	s.Handle(GET, "/debug/pprof/profile", DebugPprofProfileHandler)
	s.Handle(GET, "/debug/pprof/symbol", DebugPprofSymbolHandler)
	s.Handle(GET, "/debug/pprof/trace", DebugPprofTraceHandler)
	s.Handle(GET, "/debug/gc", GCHandler)

	return s
}

const (
	GET     HttpMethod = 1
	POST    HttpMethod = 2
	PUT     HttpMethod = 3
	DELETE  HttpMethod = 4
	HEAD    HttpMethod = 5
	OPTIONS HttpMethod = 6
	PATCH   HttpMethod = 7
)

type HttpMethod	int32

func (m *HttpMethod) name() string {
	switch *m {
	case GET:
		return "GET"
	case POST:
		return "POST"
	case PUT:
		return "PUT"
	case DELETE:
		return "DELETE"
	case HEAD:
		return "HEAD"
	case OPTIONS:
		return "OPTIONS"
	case PATCH:
		return "PATCH"
	default:
		panic("invalid http method. never happened!!!")
	}
}

type UriParams map[string]string

func (p UriParams) ByName(name string) string {
	if len(name) == 0 {
		return ""
	}
	v, ok := p[name]
	if !ok {
		return ""
	} else {
		return v
	}
}

type Handle func(http.ResponseWriter, *http.Request, UriParams)

func (s *Server) Handle(method HttpMethod, uri string, handle Handle) {
	h := func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		uriParams := make(map[string]string)
		for _, param := range params {
			if _, ok := uriParams[param.Key]; !ok {
				uriParams[param.Key] = param.Value
			}
		}
		handle(w, r, uriParams)
	}
	s.router.Handle(method.name(), uri, h)
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.closed, 0, 1) {
		// server is already closed
		return
	}

	if s.server != nil {
		s.server.Close()
	}
}

// isClosed checks whether server is closed or not.
func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.closed) == 1
}

// Run runs the server.
func (s *Server) Run() error {
	l, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		log.Error("Fail to listen:[%v]. err:%v", s.cfg.Addr, err)
		return err
	}
	if s.cfg.ConnLimit > 0 {
		l = netutil.LimitListener(l, s.cfg.ConnLimit)
	}

	s.server = &http.Server{
		Handler: s.router,
	}
	if err = s.server.Serve(l); err != nil {
		log.Error("http.listenAndServe failed: %s", err)
		return err
	}
	return nil
}

func (s *Server) Name() string {
	return s.cfg.Name
}

// Helper handlers

// Error replies to the request with the specified error message and HTTP code.
// It does not otherwise end the request; the caller should ensure no further
// writes are done to w.
// The error message should be plain text.
func Error(w http.ResponseWriter, error string, code int) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	fmt.Fprintln(w, error)
}

type ResponseWriter struct {
	http.ResponseWriter
	writer io.Writer
}

func NewResponseWriter(w http.ResponseWriter, writer io.Writer) *ResponseWriter {
	return &ResponseWriter{ResponseWriter: w, writer: writer}
}

func (w *ResponseWriter) Write(b []byte) (int, error) {
	if w.writer == nil {
		return w.Write(b)
	} else {
		return w.writer.Write(b)
	}
}

func PingPong(w http.ResponseWriter, _ *http.Request, _ UriParams) {
	w.Write([]byte("ok"))
	return
}

func DebugPprofHandler(w http.ResponseWriter, r *http.Request, _ UriParams) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("baud-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Index(ww, r)
}

func DebugPprofCmdlineHandler(w http.ResponseWriter, r *http.Request, _ UriParams) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("baud-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Cmdline(ww, r)
}

func DebugPprofProfileHandler(w http.ResponseWriter, r *http.Request, _ UriParams) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("baud-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Profile(ww, r)
}

func DebugPprofSymbolHandler(w http.ResponseWriter, r *http.Request, _ UriParams) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("baud-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Symbol(ww, r)
}

func DebugPprofTraceHandler(w http.ResponseWriter, r *http.Request, _ UriParams) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("baud-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Trace(ww, r)
}

func GCHandler(w http.ResponseWriter, _ *http.Request, _ UriParams) {
	gogc.PrintGCSummary(w)
	return
}
