package topo

// TODO: implementation for NewMasterParticipation should be abstracted from package etcd3topo with interface of backend

func (s *TopoServer) NewMasterParticipation(zone, id string) (MasterParticipation, error) {
    return s.backend.NewMasterParticipation(zone, id)
}
