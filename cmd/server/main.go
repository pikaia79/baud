package main

import (
	"fmt"
	"os"

	"gopkg.in/urfave/cli.v2"

	ps "github.com/tiglabs/baudengine/ps/server"
	"github.com/tiglabs/baudengine/util/config"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/server"
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
			s := ps.NewServer(conf)
			if err := s.Start(); err != nil {
				fmt.Printf("Baud partition server start error: %v", err)
				return err
			}

			log.Info("Baud server successful startup...")
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
		log.Error("Run server error.", "Error", err)
		os.Exit(-1)
	}
}
