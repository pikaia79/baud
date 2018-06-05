package server

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/tiglabs/baudengine/engine"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util/bytes"
	"github.com/tiglabs/baudengine/util/config"
	"github.com/tiglabs/baudengine/util/json"
	"github.com/tiglabs/baudengine/util/multierror"
)

// Config ps server config
type Config struct {
	ClusterID         string        `json:"cluster-id,omitempty"`
	NodeID            metapb.NodeID `json:"node-id,omitempty"`
	MasterServer      string        `json:"master-server,omitempty"`
	PartitionStore    string        `json:"partition-store,omitempty"`
	StoreEngine       string        `json:"store-engine,omitempty"`
	StorePath         string        `json:"store-path,omitempty"`
	StoreOption       string        `json:"store-option,omitempty"`
	DiskQuota         uint64        `json:"disk-quota,omitempty"`
	RPCPort           int           `json:"rpc-port,omitempty"`
	AdminPort         int           `json:"admin-port,omitempty"`
	HeartbeatInterval int           `json:"heartbeat-interval,omitempty"`

	RaftHeartbeatPort      int    `json:"raft-heartbeat-port,omitempty"`
	RaftReplicatePort      int    `json:"raft-replicate-port,omitempty"`
	RaftHeartbeatInterval  int    `json:"raft-heartbeat-interval,omitempty"`
	RaftRetainLogs         uint64 `json:"raft-retain-logs,omitempty"`
	RaftReplicaConcurrency int    `json:"raft-replica-concurrency,omitempty"`
	RaftSnapConcurrency    int    `json:"raft-snap-concurrency,omitempty"`

	LogDir    string `json:"log-dir,omitempty"`
	LogModule string `json:"log-module,omitempty"`
	LogLevel  string `json:"log-level,omitempty"`

	isRaftStore bool
}

// LoadConfig load server config from environment and file
func LoadConfig(conf *config.Config) *Config {
	c := &Config{}
	if conf == nil {
		return c
	}

	c.ClusterID = conf.GetString("cluster.id")
	c.MasterServer = conf.GetString("master.server")
	c.StoreEngine = conf.GetString("store.engine")
	c.StorePath = conf.GetString("store.path")
	c.StoreOption = conf.GetString("store.option")
	c.LogDir = conf.GetString("log.dir")
	c.LogModule = conf.GetString("log.module")
	c.LogLevel = conf.GetString("log.level")
	c.PartitionStore = conf.GetString("partition.store")
	if c.PartitionStore == "" {
		c.PartitionStore = "raftstore"
	}
	if c.PartitionStore == "raftstore" {
		c.isRaftStore = true
	}

	if nodeID := conf.GetString("node.id"); nodeID != "" {
		id, _ := strconv.ParseUint(nodeID, 10, 32)
		c.NodeID = metapb.NodeID(id)
	}
	if diskQuota := conf.GetString("disk.quota"); diskQuota != "" {
		c.DiskQuota, _ = strconv.ParseUint(diskQuota, 10, 64)
	}
	if rpcPort := conf.GetString("rpc.port"); rpcPort != "" {
		c.RPCPort, _ = strconv.Atoi(rpcPort)
	}
	if adminPort := conf.GetString("admin.port"); adminPort != "" {
		c.AdminPort, _ = strconv.Atoi(adminPort)
	}
	if heartbeat := conf.GetString("heartbeat.interval"); heartbeat != "" {
		c.HeartbeatInterval, _ = strconv.Atoi(heartbeat)
	}

	if raftHbPort := conf.GetString("raft.heartbeat.port"); raftHbPort != "" {
		c.RaftHeartbeatPort, _ = strconv.Atoi(raftHbPort)
	}
	if raftReplPort := conf.GetString("raft.repl.port"); raftReplPort != "" {
		c.RaftReplicatePort, _ = strconv.Atoi(raftReplPort)
	}
	if raftHbInterval := conf.GetString("raft.heartbeat.interval"); raftHbInterval != "" {
		c.RaftHeartbeatInterval, _ = strconv.Atoi(raftHbInterval)
	}
	if raftRetainLogs := conf.GetString("raft.retain.logs"); raftRetainLogs != "" {
		c.RaftRetainLogs, _ = strconv.ParseUint(raftRetainLogs, 10, 64)
	}
	if raftRepl := conf.GetString("raft.repl.concurrency"); raftRepl != "" {
		c.RaftReplicaConcurrency, _ = strconv.Atoi(raftRepl)
	}
	if raftSnap := conf.GetString("raft.snap.concurrency"); raftSnap != "" {
		c.RaftSnapConcurrency, _ = strconv.Atoi(raftSnap)
	}

	return c
}

//Validate verify the correctness of Config
func (c *Config) Validate() error {
	multierr := new(multierror.MultiError)

	if c.ClusterID == "" {
		multierr.Append(errors.New("cluster.id not specified"))
	}
	if !ExistPartitionStore(c.PartitionStore) {
		multierr.Append(fmt.Errorf("partition.store(%s) not exist", c.PartitionStore))
	}
	if c.StoreEngine == "" {
		multierr.Append(errors.New("store.engine not specified"))
	} else if !engine.Exist(c.StoreEngine) {
		multierr.Append(fmt.Errorf("store.engine(%s) not exist", c.StoreEngine))
	}
	if c.StorePath == "" {
		multierr.Append(errors.New("store.path not specified"))
	}
	if c.LogDir == "" {
		multierr.Append(errors.New("log.dir not specified"))
	}
	if c.LogModule == "" {
		multierr.Append(errors.New("log.module not specified"))
	}
	if c.LogLevel == "" {
		multierr.Append(errors.New("log.level not specified"))
	}

	if c.RPCPort <= 0 {
		multierr.Append(errors.New("rpc.port not specified"))
	}
	if c.AdminPort <= 0 {
		multierr.Append(errors.New("admin.port not specified"))
	}
	if c.HeartbeatInterval <= 0 {
		multierr.Append(errors.New("heartbeat.interval not specified"))
	}

	if c.isRaftStore {
		if c.RaftHeartbeatPort <= 0 {
			multierr.Append(errors.New("raft.heartbeat.port not specified"))
		}
		if c.RaftReplicatePort <= 0 {
			multierr.Append(errors.New("raft.repl.port not specified"))
		}
		if c.RaftHeartbeatInterval <= 0 {
			multierr.Append(errors.New("raft.heartbeat.interval not specified"))
		}
	}

	return multierr.ErrorOrNil()
}

func (c *Config) String() string {
	data, err := json.Marshal(c)
	if err != nil {
		return err.Error()
	}

	return bytes.ByteToString(data)
}
