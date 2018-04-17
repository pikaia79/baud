package server

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/tiglabs/baud/proto/metapb"
	"github.com/tiglabs/baud/util/bytes"
	"github.com/tiglabs/baud/util/config"
	"github.com/tiglabs/baud/util/json"
	"github.com/tiglabs/baud/util/log"
	"github.com/tiglabs/baud/util/multierror"
)

// Config ps server config
type Config struct {
	AppName      string
	AppVersion   string
	RPCPort      int
	AdminPort    int
	MasterServer string
	NodeID       metapb.NodeID
	DataPath     string

	LogDir    string
	LogModule string
	LogLevel  string

	RaftHeartbeatAddr       string
	RaftHeartbeatInterval   int
	RaftRetainLogs          uint64
	RaftReplicaAddr         string
	RaftReplicaConcurrency  int
	RaftSnapshotConcurrency int
}

// LoadConfig load server config
func LoadConfig(conf *config.Config) (*Config, error) {
	if conf == nil {
		return nil, errors.New("server config not specified")
	}

	c := &Config{
		RaftHeartbeatInterval:   500,
		RaftRetainLogs:          100000,
		RaftReplicaConcurrency:  20,
		RaftSnapshotConcurrency: 15,
	}
	multierr := new(multierror.MultiError)

	if c.AppName = conf.GetString("app.name"); c.AppName == "" {
		multierr.Append(errors.New("app.name not specified"))
	}
	if c.AppVersion = conf.GetString("app.version"); c.AppVersion == "" {
		multierr.Append(errors.New("app.version not specified"))
	}

	if rpcPort := conf.GetString("rpc.port"); rpcPort != "" {
		c.RPCPort, _ = strconv.Atoi(rpcPort)
	}
	if c.RPCPort <= 0 {
		multierr.Append(errors.New("rpc.port not specified"))
	}
	if adminPort := conf.GetString("admin.port"); adminPort != "" {
		c.AdminPort, _ = strconv.Atoi(adminPort)
	}
	if c.AdminPort <= 0 {
		multierr.Append(errors.New("admin.port not specified"))
	}

	if c.MasterServer = conf.GetString("master.server"); c.MasterServer == "" {
		multierr.Append(errors.New("master.server not specified"))
	}
	if c.DataPath = conf.GetString("data.path"); c.DataPath == "" {
		multierr.Append(errors.New("data.path not specified"))
	}
	if nodeID := conf.GetString("node.id"); nodeID == "" {
		if id, err := strconv.ParseUint(nodeID, 10, 32); err == nil {
			c.NodeID = metapb.NodeID(id)
		}
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

	if heartbeatPort := conf.GetString("raft.heartbeatPort"); heartbeatPort != "" {
		if port, err := strconv.Atoi(heartbeatPort); err == nil {
			c.RaftHeartbeatAddr = fmt.Sprintf(":%d", port)
		}
	}
	if c.RaftHeartbeatAddr == "" {
		multierr.Append(errors.New("raft.heartbeatPort not specified"))
	}
	if replicaPort := conf.GetString("raft.replicaPort"); replicaPort != "" {
		if port, err := strconv.Atoi(replicaPort); err == nil {
			c.RaftReplicaAddr = fmt.Sprintf(":%d", port)
		}
	}
	if c.RaftReplicaAddr == "" {
		multierr.Append(errors.New("raft.replicaPort not specified"))
	}
	if heartbeatInterval := conf.GetString("raft.heartbeatInterval"); heartbeatInterval != "" {
		if interval, err := strconv.Atoi(heartbeatInterval); err == nil {
			c.RaftHeartbeatInterval = interval
		}
	}
	if retainLogs := conf.GetString("raft.retainLogs"); retainLogs != "" {
		if retain, err := strconv.ParseUint(retainLogs, 10, 64); err == nil {
			c.RaftRetainLogs = retain
		}
	}
	if replConcurrency := conf.GetString("raft.replConcurrency"); replConcurrency != "" {
		if concurrency, err := strconv.Atoi(replConcurrency); err == nil {
			c.RaftReplicaConcurrency = concurrency
		}
	}
	if snapConcurrency := conf.GetString("raft.snapConcurrency"); snapConcurrency != "" {
		if concurrency, err := strconv.Atoi(snapConcurrency); err == nil {
			c.RaftSnapshotConcurrency = concurrency
		}
	}

	if b, err := json.Marshal(c); err == nil {
		log.Info("[Config] Loaded server config: %v", bytes.ByteToString(b))
	}

	return c, multierr.ErrorOrNil()
}
