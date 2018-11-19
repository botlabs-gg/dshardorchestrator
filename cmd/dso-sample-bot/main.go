package main

import (
	"github.com/jonas747/dshardorchestrator"
	"github.com/jonas747/dshardorchestrator/node"
	"log"
	"os"
)

func main() {
	bot := &Bot{
		token: os.Getenv("DG_TOKEN"),
	}

	_, err := node.ConnectToOrchestrator(bot, "127.0.0.1:7447", "example.1", &dshardorchestrator.StdLogger{
		Level: dshardorchestrator.LogDebug,
	})

	if err != nil {
		log.Fatal("failed connecting to orchestrator")
	}

	select {}
}
