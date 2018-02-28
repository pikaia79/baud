//the extentserver implementation
package extent

import (
	"net/http"
)

type Container struct {
	path string
}

func (s *Container) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "write"):
		s.handleWrite(w, r)
	case strings.HasPrefix(r.URL.Path, "read"):
		s.handleRead(w, r)
	case strings.HasPrefix(r.URL.Path, "truncate"):
		s.handleTruncate(w, r)
	case strings.HasPrefix(r.URL.Path, "status"):
		s.handleStatus(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof"):
		s.handlePprof(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *Containter) handleWrite(w http.ResponsWriter, r *http.Request) {}
