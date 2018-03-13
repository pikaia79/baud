package master

//the DCOS driver to allocate partitionservers etc.
type DCOS interface {
	AllocateContainer(zone string, cpu, mem, disk int) (ip string, e error)
	DistroyContainer(ip string) error
}
