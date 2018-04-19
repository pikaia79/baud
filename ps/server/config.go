package server

import (
	"errors"

	"strconv"

	"github.com/tiglabs/baud/proto/masterpb"
	"github.com/tiglabs/baud/util/bytes"
	"github.com/tiglabs/baud/util/config"
	"github.com/tiglabs/baud/util/json"
	"github.com/tiglabs/baud/util/log"
	"github.com/tiglabs/baud/util/multierror"
)

// Config ps server config
type Config struct {
	masterpb.PSConfig

	ClusterID    uint64
	MasterServer string
	DataPath     string

	LogDir    string
	LogModule string
	LogLevel  string
}

// LoadConfig load server config
func LoadConfig(conf *config.Config) (*Config, error) {
	if conf == nil {
		return nil, errors.New("server config not specified")
	}

	c := &Config{}
	multierr := new(multierror.MultiError)

	if clusterID := conf.GetString("cluster.id"); clusterID != "" {
		c.ClusterID, _ = strconv.ParseUint(clusterID, 10, 32)
	}
	if c.ClusterID == 0 {
		multierr.Append(errors.New("cluster.id not specified"))
	}
	if c.MasterServer = conf.GetString("master.server"); c.MasterServer == "" {
		multierr.Append(errors.New("master.server not specified"))
	}
	if c.DataPath = conf.GetString("data.path"); c.DataPath == "" {
		multierr.Append(errors.New("data.path not specified"))
	}

	if c.LogDir = conf.GetString("log.dir"); c.LogDir == "" {
		multierr.Append(errors.New("log.dir not specified"))
	}
	if c.LogModule = conf.GetString("log.module"); c.LogModule == "" {
		multierr.Append(errors.New("log.module not specified"))
	}
	if c.LogLevel = conf.GetString("log.level"); c.LogLevel == "" {
		multierr.Append(errors.New("log.level not specified"))
	}

	if b, err := json.Marshal(c); err == nil {
		log.Info("[Config] Loaded server config: %v", bytes.ByteToString(b))
	}

	return c, multierr.ErrorOrNil()
}
