package zm

//the DCOS driver to allocate partitionservers etc.
type DCOS interface {
	AllocateContainer(zone string, cpu, mem, disk int) (ip string, e error)
	DestroyContainer(ip string) error
}

type JDOS struct {
}

func (d *JDOS) AllocateContainer(zone string, cpu, mem, disk int) (ip string, e error) {
	return "192.168.0.1", nil
}

func (d *JDOS) DestroyContainer(ip string) error {
	return nil
}
