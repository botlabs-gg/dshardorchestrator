package tests

import (
	"github.com/jonas747/dshardorchestrator"
	"github.com/jonas747/dshardorchestrator/node"
	"github.com/jonas747/dshardorchestrator/orchestrator"
	"testing"
	"time"
)

var testServerAddr = "127.0.0.1:7447"
var testLoggerNode = &dshardorchestrator.StdLogger{Level: dshardorchestrator.LogDebug, Prefix: "node: "}
var testLoggerOrchestrator = &dshardorchestrator.StdLogger{Level: dshardorchestrator.LogDebug, Prefix: "orchestrator: "}

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
		Logger:             testLoggerOrchestrator,
	}
}

func TestEstablishSession(t *testing.T) {
	orchestrator := CreateMockOrchestrator(10)
	err := orchestrator.Start(testServerAddr)
	if err != nil {
		t.Fatal("failed starting orchestrator: ", err)
		return
	}
	defer orchestrator.Stop()

	waitChan := make(chan node.SessionInfo)
	bot := &MockBot{
		SessionEstablishedFunc: func(info node.SessionInfo) {
			waitChan <- info
		},
	}

	n, err := node.ConnectToOrchestrator(bot, testServerAddr, "testing", testLoggerNode)
	if err != nil {
		t.Fatal("failed connecting to orchestrator: ", err)
	}
	defer n.Close()

	select {
	case info := <-waitChan:
		if info.TotalShards != 10 {
			t.Error("mismatched total shards: ", info.TotalShards)
		}
	case <-time.After(time.Second * 15):
		t.Fatal("timed out waiting for session to be established")
	}
}

func TestMigrateShard(t *testing.T) {
	orchestrator := CreateMockOrchestrator(10)
	err := orchestrator.Start(testServerAddr)
	if err != nil {
		t.Fatal("failed starting orchestrator: ", err)
		return
	}
	defer orchestrator.Stop()

	// set up the sessions
	sessionWaitChan := make(chan node.SessionInfo)
	shardStartedChan := make(chan int)
	bot1 := &MockBot{
		SessionEstablishedFunc: func(info node.SessionInfo) {
			sessionWaitChan <- info
		},
		StartShardFunc: func(shard int, sessionID string, sequence int64) {
			shardStartedChan <- shard
		},
	}

	bot2 := &MockBot{
		SessionEstablishedFunc: func(info node.SessionInfo) {
			sessionWaitChan <- info
		},
		StartShardFunc: func(shard int, sessionID string, sequence int64) {
			shardStartedChan <- shard
		},
	}

	// connect node 1
	n1, err := node.ConnectToOrchestrator(bot1, testServerAddr, "testing", testLoggerNode)
	if err != nil {
		t.Fatal("failed connecting to orchestrator: ", err)
	}
	defer n1.Close()

	select {
	case info := <-sessionWaitChan:
		if info.TotalShards != 10 {
			t.Error("mismatched total shards: ", info.TotalShards)
		}
	case <-time.After(time.Second * 15):
		t.Fatal("timed out waiting for session to be established")
	}

	// connect node 2
	n2, err := node.ConnectToOrchestrator(bot2, testServerAddr, "testing", testLoggerNode)
	if err != nil {
		t.Fatal("failed connecting to orchestrator: ", err)
	}
	defer n2.Close()

	select {
	case info := <-sessionWaitChan:
		if info.TotalShards != 10 {
			t.Error("mismatched total shards: ", info.TotalShards)
		}
	case <-time.After(time.Second * 15):
		t.Fatal("timed out waiting for session to be established")
	}

	on1 := orchestrator.FindNodeByID(n1.GetIDLock())

	// start 5 shards on node 1
	for i := 0; i < 5; i++ {
		on1.StartShard(i)
		select {
		case s := <-shardStartedChan:
			if s != i {
				t.Fatal("mismatched shard id")
				return
			}
		case <-time.After(time.Second * 5):
			t.Fatal("timed out waiting for shard to start")
		}
	}

	// make sure that the orcehstrator has gotten feedback that the shards have started
	time.Sleep(time.Millisecond * 250)

	// perform the migration
	err = orchestrator.StartShardMigration(on1.Conn.GetID(), n2.GetIDLock(), 3)
	if err != nil {
		t.Fatal("failed performing migration: ", err.Error())
	}

	select {
	case s := <-shardStartedChan:
		if s != 3 {
			t.Fatal("mismatched shard id")
			return
		}
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for shard to start after migration")
	}

	// sleep another 250 milliseconds again to allow time for the shard orchestrator to acknowledge that its started
	time.Sleep(time.Millisecond * 250)

	// confirm the setup
	statuses := orchestrator.GetFullNodesStatus()
	for _, v := range statuses {
		if len(v.Shards) < 1 {
			t.Fatal("node holds no shards: ", v.ID)
		}

		if v.ID == n2.GetIDLock() {
			if len(v.Shards) != 1 {
				t.Fatal("node 2 holds incorrect number of shards: ", len(v.Shards))
			}
			if v.Shards[0] != 3 {
				t.Fatal("node 2 does not hold shard 3 after mirgation", v.Shards[0])
			}
		} else {
			if len(v.Shards) != 4 {
				t.Fatal("node 1 holds incorrect number of shards: ", len(v.Shards))
			}

			for _, s := range v.Shards {
				if s == 3 {
					t.Fatal("node 1 holds migrated shard")
				}
			}
		}
	}
}
