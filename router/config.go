package router

import (
	"github.com/BurntSushi/toml"
	"github.com/tiglabs/baud/util/log"
)

const defaultConfig = `
# Router Configuration.

role = router
ip = 0.0.0.0
httpPort = 1023
pprof = 10088
masterAddr = 1:10.1.86.118:3456
logDir = "/export/log/ps"
masterConnPoolSize = 10
psConnPoolSize = 10
`

const rpcTimeoutDef  = 100

type Config struct {
	Role               string `toml:"name,omitempty" json:"name"`
	Ip                 string
	HttpPort           uint16
	Pprof              uint16
	MasterAddr         string
	LogDir             string
	masterConnPoolSize uint16
	psConnPoolSize     uint16
}

func LoadConfig(fileName string) *Config {
	config := &Config{}
	if _, err := toml.Decode(defaultConfig, config); err != nil {
		log.Panic("decode defaultConfig failed, err %v", err)
	}
	if fileName != "" {
		if err := config.LoadFromFile(fileName); err != nil {
			log.Panic("decode %s failed, err %v", fileName, err)
		}
	}
	return config
}

func (config *Config) LoadFromFile(fileName string) error {
	if _, err := toml.DecodeFile(fileName, config); err != nil {
		return err
	}
	return config.validate()
}

func (config *Config) validate() error {
	return nil
}
