package main

import (
	"bufio"
	"github.com/jonas747/discordgo"
	"github.com/jonas747/dshardorchestrator"
	"github.com/jonas747/dshardorchestrator/orchestrator"
	"github.com/jonas747/dshardorchestrator/orchestrator/rest"
	"io"
	"log"
	"os"
	"os/exec"
	"time"
)

func main() {
	discordSession, err := discordgo.New(os.Getenv("DG_TOKEN"))
	if err != nil {
		log.Fatal("failed initilazing discord session: ", err)
	}

	orchestrator := orchestrator.NewStandardOrchestrator(discordSession)
	orchestrator.NodeLauncher = &NodeLauncher{}
	orchestrator.Logger = &dshardorchestrator.StdLogger{
		Level: dshardorchestrator.LogDebug,
	}

	orchestrator.MaxShardsPerNode = 10
	orchestrator.MaxNodeDowntimeBeforeRestart = time.Second * 10
	orchestrator.EnsureAllShardsRunning = true

	err = orchestrator.Start("127.0.0.1:7447")
	if err != nil {
		log.Fatal("failed starting orchestrator: ", err)
	}

	api := rest.NewRESTAPI(orchestrator, "127.0.0.1:7448")
	err = api.Run()
	if err != nil {
		log.Fatal("failed starting rest api: ", err)
	}

	select {}
}

type NodeLauncher struct{}

func (nl *NodeLauncher) LaunchNewNode() error {
	log.Println("starting new node...")

	cmd := exec.Command("dso-sample-bot")
	cmd.Env = os.Environ()

	wd, err := os.Getwd()
	if err != nil {
		return err
	}

	cmd.Dir = wd

	stdOut, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	go nl.PrintOutput(stdOut)

	stdErr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	go nl.PrintOutput(stdErr)

	err = cmd.Start()
	log.Println("started node: ", err)
	return err
}

func (nl *NodeLauncher) PrintOutput(reader io.Reader) {
	breader := bufio.NewReader(reader)
	for {
		s, err := breader.ReadString('\n')
		if len(s) > 0 {
			s = s[:len(s)-1]
		}

		log.Println("NODE: " + s)

		if err != nil {
			log.Println("error reading node output: ", err)
			break
		}
	}
}
