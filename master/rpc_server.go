package master

type RpcServer struct {
}

func NewRpcServer() *RpcServer {
	return new(RpcServer)
}

func (rs *RpcServer) Start() error {
	return nil
}

func (rs *RpcServer) Close() {
}
