package main

import (
	"errors"
	"fmt"
	"github.com/jedib0t/go-pretty/table"
	"github.com/jonas747/dshardorchestrator/orchestrator/rest"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

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
			Name:   "shutdownnode",
			Usage:  "shuts down a node",
			Action: ShutdownNode,
		},
		cli.Command{
			Name:   "migrateshard",
			Usage:  "migrates all shards on a node to another one",
			Action: MigrateShard,
		},
		cli.Command{
			Name:   "migratenode",
			Usage:  "migrates all shards on a node to another one",
			Action: MigrateNode,
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
			rowExtra = fmt.Sprintf("migrating %3d to   %s", n.MigratingShard, n.MigratingTo)
		}

		tb.AppendRow(table.Row{n.ID, n.Version, n.Connected, PrettyFormatNumberList(n.Shards), rowExtra})
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

func MigrateNode(c *cli.Context) error {
	args := c.Args()
	if len(args) < 2 {
		return errors.New("usage: migratenode origin-node-id target-node-id")
	}

	origin := args[0]
	target := args[1]

	fmt.Printf("migrating all shards on %s to %s, this migght take a while....\n", origin, target)

	msg, err := restClient.MigrateNode(origin, target, false)
	if err != nil {
		return err
	}

	fmt.Println(msg)
	return nil
}

func MigrateShard(c *cli.Context) error {
	args := c.Args()
	if len(args) < 2 {
		return errors.New("usage: migrateshard shard-id node-id")
	}

	shardIDStr := args[0]
	targetNode := args[1]

	shardID, err := strconv.ParseInt(shardIDStr, 10, 32)
	if err != nil {
		return err
	}

	fmt.Printf("migrating shard %d to %s...\n", shardID, targetNode)

	msg, err := restClient.MigrateShard(targetNode, int(shardID))
	if err != nil {
		return err
	}

	fmt.Println(msg)
	return nil
}

func PrettyFormatNumberList(numbers []int) string {
	if len(numbers) < 1 {
		return "None"
	}

	sort.Ints(numbers)

	var out []string

	last := 0
	seqStart := 0
	for i, n := range numbers {
		if i == 0 {
			last = n
			seqStart = n
			continue
		}

		if n > last+1 {
			// break in sequence
			if seqStart != last {
				out = append(out, fmt.Sprintf("%d - %d", seqStart, last))
			} else {
				out = append(out, fmt.Sprintf("%d", last))
			}

			seqStart = n
		}

		last = n
	}

	if seqStart != last {
		out = append(out, fmt.Sprintf("%d - %d", seqStart, last))
	} else {
		out = append(out, fmt.Sprintf("%d", last))
	}

	return strings.Join(out, ", ")
}
