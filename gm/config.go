package gm

import (
	"github.com/tiglabs/baudengine/util"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/tiglabs/baudengine/util/log"
	"os"
)

const DEFAULT_GM_CONFIG = `
# Global Master Configuration.

[module]
name = "gm"
role = "gm"
version = "v1"
# web request request signature key
signkey = ""
data-path = "/tmp/baudengine/gm1/data"

[log]
log-path = "/tmp/baudengine/gm1/log"
#debug, info, warn, error
level="debug"

[cluster]
cluster-id = "1"
gm-node-id = 1
global-etcd = "localhost:2379"
http-port=8817
rpc-port=18817
`

const (
	CONFIG_ROLE_GM = "gm"

	CONFIG_LOG_LEVEL_DEBUG = "debug"
	CONFIG_LOG_LEVEL_INFO  = "info"
	CONFIG_LOG_LEVEL_WARN  = "warn"
	CONFIG_LOG_LEVEL_ERROR = "error"
)

type Config struct {
	ModuleCfg  ModuleConfig  `toml:"module,omitempty" json:"module"`
	LogCfg     LogConfig     `toml:"log,omitempty" json:"log"`
	ClusterCfg ClusterConfig `toml:"cluster,omitempty" json:"cluster"`
}

func NewConfig(path string) *Config {
	c := new(Config)

	if _, err := toml.Decode(DEFAULT_GM_CONFIG, c); err != nil {
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
	if strings.Compare(cfg.Role, CONFIG_ROLE_GM) != 0 {
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

type ClusterConfig struct {
	ClusterID  string `toml:"cluster-id,omitempty" json:"cluster-id"`
	GmNodeId   uint64 `toml:"gm-node-id,omitempty" json:"gm-node-id"`
	GlobalEtcd string `toml:"global-etcd,omitempty" json:"global-etcd"`
	HttpPort   uint32 `toml:"http-port,omitempty" json:"http-port"`
	RpcPort    uint32 `toml:"rpc-port,omitempty" json:"rpc-port"`
}

func (cfg *ClusterConfig) adjust() {
	adjustString(&cfg.ClusterID, "no cluster-id")
	adjustUint64(&cfg.GmNodeId, "no gm-node-id")
	adjustString(&cfg.GlobalEtcd, "no global-etcd")
	adjustUint32(&cfg.HttpPort, "no http-port")
	adjustUint32(&cfg.RpcPort, "no rpc-port")
}

type LogConfig struct {
	LogPath string `toml:"log-path,omitempty" json:"log-path"`
	Level   string `toml:"level,omitempty" json:"level"`
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
