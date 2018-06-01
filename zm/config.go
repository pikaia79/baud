package zm

import (
	"github.com/tiglabs/baudengine/util"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/tiglabs/baudengine/util/log"
	"time"
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
# ZoneMaster Configuration.

[module]
name = "master"
role = "master"
version = "v1"
# web request request signature key
signkey = ""

[cluster]
cluster-id = "1"
node-id = 1
global-server-addrs = "0.0.0.0:1234"
global-root-dir = "/"

log]
log-path = "/tmp/zm_log"
#debug, info, warn, error
level="debug"
`

const (
	CONFIG_ROLE_MASTER = "master"

	CONFIG_LOG_LEVEL_DEBUG = "debug"
	CONFIG_LOG_LEVEL_INFO  = "info"
	CONFIG_LOG_LEVEL_WARN  = "warn"
	CONFIG_LOG_LEVEL_ERROR = "error"

	TOPO_TIMEOUT = 30 * time.Second
)

type Config struct {
	ModuleCfg  ModuleConfig  `toml:"module,omitempty" json:"module"`
	ClusterCfg ClusterConfig `toml:"cluster,omitempty" json:"cluster"`
	LogCfg LogConfig         `toml:"log,omitempty" json:"log"`
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
	c.ClusterCfg.adjust()
	c.LogCfg.adjust()
}

type ModuleConfig struct {
	Name     string `toml:"name,omitempty" json:"name"`
	Role     string `toml:"role,omitempty" json:"role"`
	Version  string `toml:"version,omitempty" json:"version"`
	SignKey  string `toml:"signkey,omitempty" json:"signkey"`
}

func (cfg *ModuleConfig) adjust() {
	adjustString(&cfg.Name, "no module name")

	adjustString(&cfg.Role, "no role")
	if strings.Compare(cfg.Role, CONFIG_ROLE_MASTER) != 0 {
		log.Panic("invalid role[%v]", cfg.Role)
	}
}

type ClusterNode struct {
	NodeId            string `toml:"node-id,omitempty" json:"node-id"`
	Host              string `toml:"host,omitempty" json:"host"`
	HttpPort          uint32 `toml:"http-port,omitempty" json:"http-port"` // TODO: web admin port only need one in cluster
	RpcPort           uint32 `toml:"rpc-port,omitempty" json:"rpc-port"`
}

type ClusterConfig struct {
	ZoneID            string         `toml:"zone-id,omitempty" json:"zone-id"`
	CurNodeId         string         `toml:"node-id,omitempty" json:"node-id"`
	GlobalServerAddrs string         `toml:"global-server-addrs,omitempty" json:"global-server-addrs"`
	GlobalRootDir     string         `toml:"global-root-dir,omitempty" json:"global-root-dir"`
}

type LogConfig struct {
	LogPath   string `toml:"log-path,omitempty" json:"log-path"`
	Level     string `toml:"level,omitempty" json:"level"`
}

func (cfg *LogConfig) adjust() {
	adjustString(&cfg.LogPath, "no log-path")
	adjustString(&cfg.Level, "no level")
}

func (cfg *ClusterConfig) adjust() {
	adjustString(&cfg.ZoneID, "no cluster-id")
	adjustString(&cfg.CurNodeId, "no current node-id")

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
