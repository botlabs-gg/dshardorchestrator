package main

import (
	"errors"
	"fmt"
	"github.com/jedib0t/go-pretty/table"
	"github.com/jonas747/dshardorchestrator/orchestrator/rest"
	"log"
	"os"

	"github.com/urfave/cli"
)

var restClient *rest.Client

func main() {
	app := cli.NewApp()

	app.Name = "dshardorchestrator command line client"
	app.Description = "dso-cli is a command line interface for dshardorchestrator's sharding orchestrator"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			EnvVar: "DSO_REST_SERVER_ADDR",
			Name:   "serveraddr",
			Value:  "http://127.0.0.1:7448",
		},
	}
	app.Commands = []cli.Command{
		cli.Command{
			Name:   "status",
			Usage:  "display status of all nodes",
			Action: StatusCmd,
		},
		cli.Command{
			Name:   "startnode",
			Usage:  "starts a new node",
			Action: StartNode,
		},
		cli.Command{
			Name:  "shutdownnode",
			Usage: "shuts down a node",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "node",
				},
			},
			Action: ShutdownNode,
		},
	}

	app.Before = func(c *cli.Context) error {
		restClient = rest.NewClient(c.String("serveraddr"))
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func StatusCmd(c *cli.Context) error {
	status, err := restClient.GetStatus()
	if err != nil {
		return err
	}

	tb := table.NewWriter()
	tb.AppendHeader(table.Row{"id", "version", "connected", "shards", "migrating"})

	for _, n := range status.Nodes {

		rowExtra := ""

		if n.MigratingFrom != "" {
			rowExtra = fmt.Sprintf("migrating %3d from %s", n.MigratingShard, n.MigratingFrom)
		} else if n.MigratingTo != "" {
			rowExtra = fmt.Sprintf("migrating %3d to %s", n.MigratingShard, n.MigratingTo)
		}

		tb.AppendRow(table.Row{n.ID, n.Version, n.Connected, len(n.Shards), rowExtra})
	}

	fmt.Println(tb.Render())
	return nil
}

func StartNode(c *cli.Context) error {
	msg, err := restClient.StartNewNode()
	if err != nil {
		return err
	}

	fmt.Println(msg)
	return nil
}

func ShutdownNode(c *cli.Context) error {
	args := c.Args()
	if len(args) < 1 || args[0] == "" {
		return errors.New("no node specified")
	}

	fmt.Println("shutting down " + args[0])
	msg, err := restClient.ShutdownNode(args[0])
	if err != nil {
		return err
	}

	fmt.Println(msg)
	return nil
}
