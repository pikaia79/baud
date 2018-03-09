package master

//master config
type Cfg struct {
	clusterID string
	httpPort  string
	dataPath  string
	raftPath  string
	logPath   string

	nodeID int
}
