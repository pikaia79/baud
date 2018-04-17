package telnet


import (
	"crypto/tls"
	"net"
	"errors"
)

// Program register CmdHandler for processing incoming TELNET command
// Session is communication context to decode/encode and other process
type TelnetCmdCB func(Session, string, []string) error

type cmdInfo struct {
	cb TelnetCmdCB
	helpStr string
}

type Server struct {
	Addr    	string  // TCP address to listen on; ":telnet" or ":telnets" if empty (when used with ListenAndServe or ListenAndServeTLS respectively).
	Logger		Logger
	TLSConfig 	*tls.Config // optional TLS configuration; used by ListenAndServeTLS.
	Prompt string

	registeredCmdMap map[string]*cmdInfo
}

// Register a command to process telnet command
func (server *Server) RegisterCmd(cmdName, helpStr string, cb TelnetCmdCB) error {
	if _, ok := server.registeredCmdMap[cmdName]; ok {
		return errors.New("command already registered")
	}
	server.registeredCmdMap[cmdName] = &cmdInfo{cb: cb, helpStr: helpStr}
	return nil
}

// ListenAndServe listens on the TCP network address 'server.Addr' and then spawns a call to the ServeTELNET
func (server *Server) ListenAndServe() error {
	addr := server.Addr
	if "" == addr {
		addr = ":telnet"
	}

	listener, err := net.Listen("tcp", addr)
	if nil != err {
		return err
	}

	return server.Serve(listener)
}


// Serve accepts an incoming TELNET client connection on the net.Listener `listener`.
func (server *Server) Serve(listener net.Listener) error {
	defer listener.Close()

	logger := server.logger()
	if server.Prompt == "" {
		server.Prompt = "> "
	}

	for {
		// Wait for a new TELNET client connection.
		logger.Debugf("Listening at %q.", listener.Addr())
		conn, err := listener.Accept()
		if err != nil {
//@TODO: Could try to recover from certain kinds of errors. Maybe waiting a while before trying again.
			return err
		}
		logger.Debugf("Received new connection from %q.", conn.RemoteAddr())

		// Handle the new TELNET client connection by spawning
		// a new goroutine.
		go func() {
			session := NewSession(server.Prompt, conn, func(session Session, cmdName string, params []string) error {
				cmdInfo, ok := server.registeredCmdMap[cmdName]
				if !ok {
					return errors.New("cannot found telnet cmd: " + cmdName)
				}
				return cmdInfo.cb(session, cmdName, params)
			})
			session.SetLogger(server.logger())
		}()
		logger.Debugf("Spawned handler to handle connection from %q.", conn.RemoteAddr())
	}
}

func (server *Server) logger() Logger {
	logger := server.Logger
	if nil == logger {
		logger = internalDiscardLogger{}
	}

	return logger
}

