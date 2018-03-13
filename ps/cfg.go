package ps

//partitionserver config
type Cfg struct {
	serverID string
	httpPort string
	raftPort string
	dataPath string
	raftPath string
	logPath  string
}
