package master

import (
	"github.com/tiglabs/baudengine/util"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/tiglabs/baudengine/util/log"
	"os"
)

//const (
//	defaultServerName           = "master"
//	defaultMaxReplicas          = 3
//	defaultMaxSnapshotCount     = 3
//	defaultMaxNodeDownTime      = time.Hour
//	defaultLeaderScheduleLimit  = 64
//	defaultRegionScheduleLimit  = 12
//	defaultReplicaScheduleLimit = 16
//	defaultRaftHbInterval       = time.Millisecond * 500
//	defaultRaftRetainLogsCount  = 100
//	defaultMaxTaskWaitTime      = 5 * time.Minute
//	defaultMaxRangeDownTime     = 10 * time.Minute
//)

const DEFAULT_MASTER_CONFIG = `
# Master Configuration.

[module]
name = "master"
role = "master"
version = "v1"
# web request request signature key
signkey = ""
data-path = "/tmp/baudengine/master/data"

[log]
log-path = "/tmp/baudengine/master/log"
#debug, info, warn, error
level="debug"
#debug, info, warn
raft-level="info"

[cluster]
cluster-id = "1"
node-id = 1
raft-heartbeat-interval="500ms"
raft-retain-logs-count=100

[[cluster.nodes]]
node-id = 1
http-port = 8887
rpc-port = 18887
raft-heartbeat-port=8886
raft-replicate-port=8885

[[cluster.nodes]]
node-id = 2
http-port = 8897
rpc-port = 18897
raft-heartbeat-port=8896
raft-replicate-port=8895

[ps]
rpc-port=8000
admin-port=8001
heartbeat-interval="100ms"
raft-heartbeat-port=8002
raft-replicate-port=8003
raft-retain-logs=10000
raft-replica-concurrency=1
raft-snapshot-concurrency=1
`

const (
	CONFIG_ROLE_MASTER = "master"

	CONFIG_LOG_LEVEL_DEBUG = "debug"
	CONFIG_LOG_LEVEL_INFO  = "info"
	CONFIG_LOG_LEVEL_WARN  = "warn"
	CONFIG_LOG_LEVEL_ERROR = "error"
)

type Config struct {
	ModuleCfg  ModuleConfig  `toml:"module,omitempty" json:"module"`
	LogCfg     LogConfig     `toml:"log,omitempty" json:"log"`
	ClusterCfg ClusterConfig `toml:"cluster,omitempty" json:"cluster"`
	PsCfg      PsConfig      `toml:"ps,omitempty" json:"ps"`
}

func NewConfig(path string) *Config {
	c := new(Config)

	if _, err := toml.Decode(DEFAULT_MASTER_CONFIG, c); err != nil {
		log.Panic("fail to decode default config, err[%v]", err)
	}

	if len(path) != 0 {
		_, err := toml.DecodeFile(path, c)
		if err != nil {
			log.Panic("fail to decode config file[%v]. err[v]", path, err)
		}
	}

	c.adjust()

	return c
}

func (c *Config) adjust() {
	c.ModuleCfg.adjust()
	c.LogCfg.adjust()
	c.ClusterCfg.adjust()
	c.PsCfg.adjust()
}

type ModuleConfig struct {
	Name     string `toml:"name,omitempty" json:"name"`
	Role     string `toml:"role,omitempty" json:"role"`
	Version  string `toml:"version,omitempty" json:"version"`
	SignKey  string `toml:"signkey,omitempty" json:"signkey"`
	DataPath string `toml:"data-path,omitempty" json:"data-path"`
}

func (cfg *ModuleConfig) adjust() {
	adjustString(&cfg.Name, "no module name")

	adjustString(&cfg.Role, "no role")
	if strings.Compare(cfg.Role, CONFIG_ROLE_MASTER) != 0 {
		log.Panic("invalid role[%v]", cfg.Role)
	}

	adjustString(&cfg.DataPath, "no data path")
	_, err := os.Stat(cfg.DataPath)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(cfg.DataPath, os.ModePerm); err != nil {
			log.Panic("fail to create meta data path[%v]. err[%v]", cfg.DataPath, err)
		}
	}
}

type ClusterNode struct {
	NodeId            uint64 `toml:"node-id,omitempty" json:"node-id"`
	Host              string `toml:"host,omitempty" json:"host"`
	HttpPort          uint32 `toml:"http-port,omitempty" json:"http-port"` // TODO: web admin port only need one in cluster
	RpcPort           uint32 `toml:"rpc-port,omitempty" json:"rpc-port"`
	RaftHeartbeatPort uint32 `toml:"raft-heartbeat-port,omitempty" json:"raft-heartbeat-port"`
	RaftReplicatePort uint32 `toml:"raft-replicate-port,omitempty" json:"raft-replicate-port"`
}

type ClusterConfig struct {
	ClusterID             string         `toml:"cluster-id,omitempty" json:"cluster-id"`
	CurNodeId             uint64         `toml:"node-id,omitempty" json:"node-id"`
	RaftHeartbeatInterval util.Duration  `toml:"raft-heartbeat-interval,omitempty" json:"raft-heartbeat-interval"`
	RaftRetainLogsCount   uint64         `toml:"raft-retain-logs-count,omitempty" json:"raft-retain-logs-count"`
	Nodes                 []*ClusterNode `toml:"nodes,omitempty" json:"nodes"`
	CurNode               *ClusterNode
}

func (cfg *ClusterConfig) adjust() {
	adjustString(&cfg.ClusterID, "no cluster-id")
	adjustUint64(&cfg.CurNodeId, "no current node-id")
	adjustDuration(&cfg.RaftHeartbeatInterval, "no raft heartbeat interval")
	adjustUint64(&cfg.RaftRetainLogsCount, "no raft retain log count")

	if len(cfg.Nodes) == 0 {
		log.Panic("cluster nodes is empty")
	}

	// validate whether is node-id duplicated
	tempNodes := make(map[uint64]*ClusterNode)

	for _, node := range cfg.Nodes {
		adjustUint64(&node.NodeId, "no node-id")
		adjustString(&node.Host, "no node host")

		adjustUint32(&node.HttpPort, "no node http port")
		if node.HttpPort <= 1024 || node.HttpPort > 65535 {
			log.Panic("out of node http port %d", node.HttpPort)
		}

		adjustUint32(&node.RpcPort, "no node rpc port")
		if node.RpcPort <= 1024 || node.RpcPort > 65535 {
			log.Panic("out of node rpc port %d", node.RpcPort)
		}

		adjustUint32(&node.RaftHeartbeatPort, "no node raft heartbeat port")
		if node.RaftHeartbeatPort <= 1024 || node.RaftHeartbeatPort > 65535 {
			log.Panic("out of node raft heartbeat port %d", node.RaftHeartbeatPort)
		}

		adjustUint32(&node.RaftReplicatePort, "no node raft replicate port")
		if node.RaftReplicatePort <= 1024 || node.RaftReplicatePort > 65535 {
			log.Panic("out of node raft replicate port %d", node.RaftReplicatePort)
		}

		if _, ok := tempNodes[node.NodeId]; ok {
			log.Panic("duplicated node-id[%v]", node.NodeId)
		}
		tempNodes[node.NodeId] = node

		if node.NodeId == cfg.CurNodeId {
			cfg.CurNode = node
		}
	}
}

type LogConfig struct {
	LogPath   string `toml:"log-path,omitempty" json:"log-path"`
	Level     string `toml:"level,omitempty" json:"level"`
	RaftLevel string `toml:"raft-level,omitempty" json:"raft-level"`
}

func (c *LogConfig) adjust() {
	adjustString(&c.LogPath, "no log path")
	_, err := os.Stat(c.LogPath)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(c.LogPath, os.ModePerm); err != nil {
			log.Panic("fail to create log path[%v]. err[%v]", c.LogPath, err)
		}
	}

	adjustString(&c.Level, "no log level")
	c.Level = strings.ToLower(c.Level)
	switch c.Level {
	case CONFIG_LOG_LEVEL_DEBUG:
	case CONFIG_LOG_LEVEL_INFO:
	case CONFIG_LOG_LEVEL_WARN:
	case CONFIG_LOG_LEVEL_ERROR:
	default:
		log.Panic("Invalid log level[%v]", c.Level)
	}

	adjustString(&c.RaftLevel, "no raft log level")
	c.RaftLevel = strings.ToLower(c.RaftLevel)
	switch c.RaftLevel {
	case CONFIG_LOG_LEVEL_DEBUG:
	case CONFIG_LOG_LEVEL_INFO:
	case CONFIG_LOG_LEVEL_WARN:
	default:
		log.Panic("Invalid raft log level[%v]", c.RaftLevel)
	}
}

type PsConfig struct {
	RpcPort                 uint32        `toml:"rpc-port,omitempty" json:"rpc-port"`
	AdminPort               uint32        `toml:"admin-port,omitempty" json:"admin-port"`
	HeartbeatInterval       util.Duration `toml:"heartbeat-interval,omitempty" json:"heartbeat-interval"`
	RaftHeartbeatPort       uint32        `toml:"raft-heartbeat-port,omitempty" json:"raft-heartbeat-port"`
	RaftReplicatePort       uint32        `toml:"raft-replicate-port,omitempty" json:"raft-replicate-port"`
	RaftRetainLogs          uint64        `toml:"raft-retain-logs,omitempty" json:"raft-retain-logs"`
	RaftReplicaConcurrency  uint32        `toml:"raft-replica-concurrency,omitempty" json:"raft-replica-concurrency"`
	RaftSnapshotConcurrency uint32        `toml:"raft-snapshot-concurrency,omitempty" json:"raft-snapshot-concurrency"`
}

func (cfg *PsConfig) adjust() {
	adjustUint32(&cfg.RpcPort, "no ps raft port")
	adjustUint32(&cfg.AdminPort, "no ps admin port")
	adjustDuration(&cfg.HeartbeatInterval, "no ps heartbeat interval")
	adjustUint32(&cfg.RaftHeartbeatPort, "no ps raft heartbeat port")
	adjustUint32(&cfg.RaftReplicatePort, "no ps raft replicate port")
	adjustUint64(&cfg.RaftRetainLogs, "no ps raft retain logs")
	adjustUint32(&cfg.RaftReplicaConcurrency, "no ps raft replicate concurrency")
	adjustUint32(&cfg.RaftSnapshotConcurrency, "no ps raft snapshot concurrency")
}

func adjustString(v *string, errMsg string) {
	if len(*v) == 0 {
		log.Panic("Config adjust string error, %v", errMsg)
	}
}

func adjustUint32(v *uint32, errMsg string) {
	if *v == 0 {
		log.Panic("Config adjust uint32 error, %v", errMsg)
	}
}

func adjustUint64(v *uint64, errMsg string) {
	if *v == 0 {
		log.Panic("Config adjust uint64 error, %v", errMsg)
	}
}

func adjustDuration(v *util.Duration, errMsg string) {
	if v.Duration == 0 {
		log.Panic("Config adjust duration error, %v", errMsg)
	}
}
