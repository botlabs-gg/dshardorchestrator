package tests

import (
	"github.com/jonas747/dshardorchestrator"
	"github.com/jonas747/dshardorchestrator/node"
	"github.com/jonas747/dshardorchestrator/orchestrator"
	"testing"
	"time"
)

var testServerAddr = "127.0.0.1:7447"
var testLogger = &dshardorchestrator.StdLogger{Level: dshardorchestrator.LogDebug}

type mockShardCountProvider struct {
	NumShards int
}

func (m *mockShardCountProvider) GetTotalShardCount() (int, error) {
	return m.NumShards, nil
}

func CreateMockOrchestrator(numShards int) *orchestrator.Orchestrator {
	return &orchestrator.Orchestrator{
		ShardCountProvider: &mockShardCountProvider{numShards},
		NodeIDProvider:     orchestrator.NewNodeIDProvider(),
		Logger:             testLogger,
	}
}

func TestEstablishSession(t *testing.T) {
	orchestrator := CreateMockOrchestrator(10)
	err := orchestrator.Start(testServerAddr)
	if err != nil {
		t.Fatal("failed starting orchestrator: ", err)
		return
	}

	waitChan := make(chan node.SessionInfo)
	bot := &MockBot{
		SessionEstablishedFunc: func(info node.SessionInfo) {
			waitChan <- info
		},
	}

	_, err = node.ConnectToOrchestrator(bot, testServerAddr, "testing", testLogger)
	if err != nil {
		t.Fatal("failed connecting to orchestrator: ", err)
	}

	select {
	case info := <-waitChan:
		if info.TotalShards != 10 {
			t.Error("mismatched total shards: ", info.TotalShards)
		}
	case <-time.After(time.Second * 15):
		t.Fatal("timed out waiting for session to be established")
	}
}
