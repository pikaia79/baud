package master

type Selector interface {
	SelectTarget(servers []*PartitionServer) *PartitionServer
}

type IdleSelector struct {

}

func NewIdleSelector() Selector {
	return &IdleSelector{
	}
}

func (s *IdleSelector) SelectTarget(servers []*PartitionServer) *PartitionServer {
	if servers == nil {
		return nil
	}

	// TODO: prevent form selecting same one server at many times
	var result *PartitionServer
	for _, server := range servers {
		if !server.isReplicaFull() {
			result = server
			break
		}
	}

	return result
}
