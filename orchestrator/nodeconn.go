package orchestrator

import (
	"github.com/jonas747/dshardorchestrator"
	"github.com/sirupsen/logrus"
	"net"
	"strings"
	"sync"
	"time"
)

// Represents a connection from a master server to a slave
type NodeConn struct {
	Orchestrator *Orchestrator
	Conn         *dshardorchestrator.Conn

	// below fields are protected by this mutex
	mu sync.Mutex

	sessionEstablished bool

	runningShards []int

	shardMigrationOtherNodeID   string
	shardMigrationShard         bool
	shardMigrationMode          dshardorchestrator.ShardMigrationMode
	processedUserEvents         int
	shardmigrationTotalUserEvts int

	shuttingDown bool
}

// NewNodeConn creates a new NodeConn (connection from master to slave) from a net.Conn
func (o *Orchestrator) NewNodeConn(netConn net.Conn) *NodeConn {
	sc := &NodeConn{
		Conn:         dshardorchestrator.ConnFromNetCon(netConn),
		Orchestrator: o,
	}

	sc.Conn.MessageHandler = sc.handleMessage
	sc.Conn.ConnClosedHanlder = func() {
		// TODO
	}

	return sc
}

func (nc *NodeConn) listen() {
	nc.Conn.Listen()
}

// Handle incoming messages
func (nc *NodeConn) handleMessage(msg *dshardorchestrator.Message) {
	switch msg.EvtID {
	case dshardorchestrator.EvtIdentify:
		nc.handleIdentify(msg.DecodedBody.(*dshardorchestrator.IdentifyData))

	case dshardorchestrator.EvtStartShard:
		data := msg.DecodedBody.(*dshardorchestrator.StartShardData)
		nc.mu.Lock()
		nc.runningShards = append(nc.runningShards, data.ShardID)
		nc.mu.Unlock()

	case dshardorchestrator.EvtStopShard:
		data := msg.DecodedBody.(*dshardorchestrator.StopShardData)
		nc.mu.Lock()
		for i, v := range nc.runningShards {
			if v == data.ShardID {
				nc.runningShards = append(nc.runningShards[:i], nc.runningShards[i+1:])
			}
		}
		nc.mu.Unlock()

	case dshardorchestrator.EvtPrepareShardmigration:
		data := msg.DecodedBody.(*dshardorchestrator.PrepareShardmigrationData)

		nc.mu.Lock()
		otherNodeID := nc.otherNodeID
		nc.mu.Unlock()

		otherNode := nc.Orchestrator.FindNodeByID(id)
		if otherNode == nil {
			logrus.Error("node dissapeared in the middle of shard migration")
			return
		}

		if data.Origin {
			data.Origin = false
			go otherNode.Conn.SendLogErr(dshardorchestrator.EvtPrepareShardmigration, data)
			return
		}

		// start sending state data
		go otherNode.Conn.SendLogErr(dshardorchestrator.EvtStartShardMigration, &dshardorchestrator.StartshardMigrationData{
			ShardID: data.ShardID,
		})

	default:
		if msg.EvtID < 100 {
			return
		}

		nc.mu.Lock()
		otherNodeID := nc.otherNodeID
		nc.mu.Unlock()

		otherNode := nc.Orchestrator.FindNodeByID(id)
		if otherNode == nil {
			logrus.Error("node dissapeared in the middle of shard migration")
			return
		}

		go otherNode.Conn.SendLogErr(msg.EvtID, msg.RawBody)
	}

	// mu.Lock()
	// defer mu.Unlock()

	// switch msg.EvtID {
	// case EvtSlaveHello:
	// 	hello := dataInterface.(*SlaveHelloData)
	// 	s.handleHello(hello)

	// // Full slave migration with shard rescaling not implemented yet
	// // case EvtSoftStartComplete:
	// // 	go mainSlave.Conn.Send(EvtShutdown, nil)

	// case EvtShardMigrationStart:
	// 	data := dataInterface.(*ShardMigrationStartData)
	// 	if data.FromThisSlave {
	// 		logrus.Println("Main slave is ready for migration, readying slave, numshards: ", data.NumShards)
	// 		// The main slave is ready for migration, prepare the new slave
	// 		data.FromThisSlave = false

	// 		go newSlave.Conn.SendLogErr(EvtShardMigrationStart, data)
	// 	} else {
	// 		logrus.Println("Both slaves are ready for migration, starting with shard 0")
	// 		// Both slaves are ready, start the transfer
	// 		go mainSlave.Conn.SendLogErr(EvtStopShard, &StopShardData{Shard: 0})
	// 	}

	// case EvtStopShard:
	// 	// The main slave stopped a shard, resume it on the new slave
	// 	data := dataInterface.(*StopShardData)

	// 	logrus.Printf("Shard %d stopped, sending resume on new slave... (%d, %s) ", data.Shard, data.Sequence, data.SessionID)

	// 	go newSlave.Conn.SendLogErr(EvtResume, &ResumeShardData{
	// 		Shard:     data.Shard,
	// 		SessionID: data.SessionID,
	// 		Sequence:  data.Sequence,
	// 	})

	// case EvtResume:
	// 	data := dataInterface.(*ResumeShardData)
	// 	logrus.Printf("Shard %d resumed, Stopping next shard", data.Shard)

	// 	data.Shard++
	// 	go mainSlave.Conn.SendLogErr(EvtStopShard, &StopShardData{
	// 		Shard:     data.Shard,
	// 		SessionID: data.SessionID,
	// 		Sequence:  data.Sequence,
	// 	})

	// case EvtGuildState:
	// 	newSlave.Conn.Send(EvtGuildState, msg.Body)
	// }
}

func (nc *NodeConn) handleIdentify(data *dshardorchestrator.IdentifyData) {
	nc.Orchestrator.mu.Lock()
	if data.TotalShards == 0 && nc.Orchestrator.totalShards == 0 {
		// we may need to fetch a fresh shard count, but wait 10 seconds to see if another node with already set shard count connects

		nc.Orchestrator.mu.Unlock()
		for i := 0; i < 100; i++ {
			time.Sleep(time.Millisecond * 100)

			nc.Orchestrator.mu.Lock()
			if nc.Orchestrator.totalShards != 0 {
				nc.Orchestrator.mu.Unlock()
				break
			}

			nc.Orchestrator.mu.Unlock()
		}

		// we need to fetch a fresh total shard count
		for {
			nc.Orchestrator.mu.Lock()
			if nc.Orchestrator.totalShards != 0 {
				break
			}

			sc, err := nc.Orchestrator.ShardCountProvider.GetTotalShardCount()
			if err != nil {
				nc.Orchestrator.mu.Unlock()
				logrus.WithError(err).Error("failed fetching total shard count, retrying in a second")
				time.Sleep(time.Second)
				continue
			}

			nc.Orchestrator.totalShards = sc
			break // keep it locked out of the loop
		}
	}

	totalShards := nc.Orchestrator.totalShards
	if data.TotalShards > 0 && nc.Orchestrator.totalShards == 0 {
		nc.Orchestrator.totalShards = data.TotalShards
		totalShards = data.TotalShards
	} else if data.TotalShards > 0 && data.TotalShards != nc.Orchestrator.totalShards {
		// in this case there isn't much we can do, in the current state the orchestrator does not support varying shard counts so if this were to happen then yeah...
		// in the future this will be handled, things like rescaling shard by doubling the count is a relatively easy process
		// (shut down 1 shard completely, start 2 shards that combined were holding the same servers as the one shut down, works since it's doubled)
		logrus.Error("NOT-MATCHING SHARD COUNTS!")
		nc.Orchestrator.mu.Unlock()
		return
	}

	nc.Orchestrator.mu.Unlock()

	// check if this connection holds a "preliminary" id instead of a global unique one
	if strings.HasPrefix(data.NodeID, "unknown") {
		newID := nc.Orchestrator.NodeIDProvider.GenerateID()
		nc.Conn.ID.Store(newID)
	} else {
		nc.Conn.ID.Store(data.NodeID)
	}

	// after this we have sucessfully established a session
	resp := &dshardorchestrator.IdentifiedData{
		NodeID:      nc.Conn.GetID(),
		TotalShards: totalShards,
	}

	go nc.Conn.SendLogErr(dshardorchestrator.EvtIdentified, resp)

	nc.mu.Lock()
	nc.sessionEstablished = true
	nc.mu.Unlock()
}

// GetFullStatus returns the current status of the node
func (nc *NodeConn) GetFullStatus() *NodeStatus {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	status := &NodeStatus{
		ID:                 nc.Conn.GetID(),
		SessionEstablished: nc.sessionEstablished,
		MigratingShard:     nc.shardMigrationShard,
	}

	status.runningShards = make([]int, len(nc.runningShards))
	copy(status.runningShards, nc.runningShards)

	if nc.shardMigrationMode == dshardorchestrator.ShardMigrationModeFrom {
		status.MigratingTo = nc.shardMigrationOtherNodeID
	} else if nc.shardMigrationMode == dshardorchestrator.ShardMigrationModeTo {
		status.MigratingFrom = nc.shardMigrationOtherNodeID
	}

	return status
}
