package main

import (
	"fmt"
	"os"
	"path"

	"gopkg.in/urfave/cli.v2"

	ps "github.com/tiglabs/baudengine/ps/server"
	_ "github.com/tiglabs/baudengine/ps/storage/raftstore"
	"github.com/tiglabs/baudengine/util/config"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/server"
	"github.com/tiglabs/raft/logger"
	raftlog "github.com/tiglabs/raft/util/log"
)

const (
	flagConfig = "config"
)

var (
	app = &cli.App{
		Name:        "baud-server",
		Usage:       "baud-server [command]",
		Description: "Baud engine partition server.",
	}
	startCmd = &cli.Command{
		Name:        "start",
		Usage:       "baud-server start",
		Description: "Start the baud server",
		Action: func(cmdCtx *cli.Context) error {
			// set go flag values
			server.SetGoFlagVals(cmdCtx)

			conf := config.LoadConfigFile(cmdCtx.String(flagConfig))
			serverConf := ps.LoadConfig(conf)
			if err := serverConf.Validate(); err != nil {
				fmt.Printf("Baud partition server start error: %s", err)
				return err
			}
			fmt.Printf("Server start with config is: %s", serverConf)

			// init log
			log.InitFileLog(serverConf.LogDir, serverConf.LogModule, serverConf.LogLevel)
			// init raft log
			if raftLog, err := raftlog.NewLog(path.Join(serverConf.LogDir, "raft"), "raft", serverConf.LogLevel); err == nil {
				logger.SetLogger(raftLog)
			}

			s := ps.NewServer(serverConf)
			if err := s.Start(); err != nil {
				fmt.Printf("Baud partition server start error: %s", err)
				return err
			}

			server.WaitShutdown(s.Close)
			return nil
		},
	}
)

func init() {
	server.AppendFlags(startCmd, &cli.StringFlag{
		Name:    flagConfig,
		Aliases: []string{"c"},
		Usage:   fmt.Sprintf("server config file path"),
	})

	// add go flags to start command
	server.AddGoFlags(startCmd)
	app.Commands = append(app.Commands, startCmd)
	app.Commands = append(app.Commands, server.VersionCommand())
}

func main() {
	// Needed to avoid "logging before flag.Parse" error with glog.
	server.SupressGlogWarnings()
	if err := app.Run(os.Args); err != nil {
		fmt.Printf("Run baud server error: %s", err)
		os.Exit(-1)
	}
}
