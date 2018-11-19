package orchestrator

import (
	"fmt"
	"github.com/jonas747/dshardorchestrator"
	"net"
	"strings"
	"sync"
	"time"
)

// Represents a connection from a master server to a slave
type NodeConn struct {
	Orchestrator *Orchestrator
	Conn         *dshardorchestrator.Conn

	connected      bool
	disconnectedAt time.Time

	// below fields are protected by this mutex
	mu sync.Mutex

	sessionEstablished bool
	version            string
	runningShards      []int

	shardMigrationOtherNodeID   string
	shardMigrationShard         int
	shardMigrationMode          dshardorchestrator.ShardMigrationMode
	processedUserEvents         int
	shardmigrationTotalUserEvts int

	shuttingDown bool
}

// NewNodeConn creates a new NodeConn (connection from master to slave) from a net.Conn
func (o *Orchestrator) NewNodeConn(netConn net.Conn) *NodeConn {
	sc := &NodeConn{
		Conn:         dshardorchestrator.ConnFromNetCon(netConn, o.Logger),
		Orchestrator: o,
		connected:    true,
	}

	sc.Conn.MessageHandler = sc.handleMessage
	sc.Conn.ConnClosedHanlder = func() {
		// TODO
		sc.mu.Lock()
		sc.connected = false
		sc.disconnectedAt = time.Now()
		sc.mu.Unlock()
	}

	return sc
}

func (nc *NodeConn) listen() {
	nc.Conn.Listen()
}

func (nc *NodeConn) removeShard(shardID int) {
	for i, v := range nc.runningShards {
		if v == shardID {
			nc.runningShards = append(nc.runningShards[:i], nc.runningShards[i+1:]...)
		}
	}
}

// Handle incoming messages
func (nc *NodeConn) handleMessage(msg *dshardorchestrator.Message) {
	switch msg.EvtID {
	case dshardorchestrator.EvtIdentify:
		nc.handleIdentify(msg.DecodedBody.(*dshardorchestrator.IdentifyData))

	case dshardorchestrator.EvtStartShard:
		data := msg.DecodedBody.(*dshardorchestrator.StartShardData)
		nc.mu.Lock()
		if !dshardorchestrator.ContainsInt(nc.runningShards, data.ShardID) {
			nc.runningShards = append(nc.runningShards, data.ShardID)
		}
		nc.mu.Unlock()

	case dshardorchestrator.EvtStopShard:
		data := msg.DecodedBody.(*dshardorchestrator.StopShardData)
		nc.mu.Lock()
		nc.removeShard(data.ShardID)
		nc.mu.Unlock()

	case dshardorchestrator.EvtPrepareShardmigration:
		data := msg.DecodedBody.(*dshardorchestrator.PrepareShardmigrationData)

		nc.mu.Lock()
		otherNodeID := nc.shardMigrationOtherNodeID
		nc.mu.Unlock()

		otherNode := nc.Orchestrator.FindNodeByID(otherNodeID)
		if otherNode == nil {
			nc.Conn.Log(dshardorchestrator.LogError, nil, "node dissapeared in the middle of shard migration")
			return
		}

		if data.Origin {
			nc.mu.Lock()
			nc.removeShard(data.ShardID)
			nc.mu.Unlock()

			data.Origin = false
			go otherNode.Conn.SendLogErr(dshardorchestrator.EvtPrepareShardmigration, data)
			return
		}
		// start sending state data
		go otherNode.Conn.SendLogErr(dshardorchestrator.EvtStartShardMigration, &dshardorchestrator.StartshardMigrationData{
			ShardID: data.ShardID,
		})

	case dshardorchestrator.EvtAllUserdataSent:
		nc.mu.Lock()
		otherNodeID := nc.shardMigrationOtherNodeID
		nc.mu.Unlock()

		otherNode := nc.Orchestrator.FindNodeByID(otherNodeID)
		if otherNode == nil {
			nc.Conn.Log(dshardorchestrator.LogError, nil, "node dissapeared in the middle of shard migration")
			return
		}

		go otherNode.Conn.SendLogErr(dshardorchestrator.EvtAllUserdataSent, msg.DecodedBody)

	default:
		if msg.EvtID < 100 {
			return
		}

		nc.mu.Lock()
		otherNodeID := nc.shardMigrationOtherNodeID
		nc.mu.Unlock()

		otherNode := nc.Orchestrator.FindNodeByID(otherNodeID)
		if otherNode == nil {
			nc.Conn.Log(dshardorchestrator.LogError, nil, "node dissapeared in the middle of shard migration")
			return
		}

		go otherNode.Conn.SendLogErr(msg.EvtID, msg.RawBody)
	}
}

func (nc *NodeConn) validateTotalShards(data *dshardorchestrator.IdentifyData) (ok bool, totalShards int) {
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
				nc.Conn.Log(dshardorchestrator.LogError, err, "failed fetching total shard count, retrying in a second")
				time.Sleep(time.Second)
				continue
			}

			nc.Orchestrator.totalShards = sc
			break // keep it locked out of the loop
		}
	}

	totalShards = nc.Orchestrator.totalShards
	if data.TotalShards > 0 && nc.Orchestrator.totalShards == 0 {
		nc.Orchestrator.totalShards = data.TotalShards
		totalShards = data.TotalShards
	} else if data.TotalShards > 0 && data.TotalShards != nc.Orchestrator.totalShards {
		// in this case there isn't much we can do, in the current state the orchestrator does not support varying shard counts so if this were to happen then yeah...
		// in the future this will be handled, things like rescaling shard by doubling the count is a relatively easy process
		// (shut down 1 shard completely, start 2 shards that combined were holding the same servers as the one shut down, works since it's doubled)
		nc.Conn.Log(dshardorchestrator.LogError, nil, "NOT-MATCHING TOTAL SHARD COUNTS!")
		nc.Orchestrator.mu.Unlock()
		return false, totalShards
	}

	nc.Orchestrator.mu.Unlock()
	return true, totalShards
}

func (nc *NodeConn) handleIdentify(data *dshardorchestrator.IdentifyData) {
	valid, totalShards := nc.validateTotalShards(data)
	if !valid {
		return
	}

	// check if this connection holds a "preliminary" id instead of a global unique one
	if strings.HasPrefix(data.NodeID, "unknown") {
		newID := nc.Orchestrator.NodeIDProvider.GenerateID()
		nc.Conn.ID.Store(newID)
	} else {
		nc.Conn.ID.Store(data.NodeID)

		// check if were holding a duplicates
		nc.Orchestrator.mu.Lock()
		for i, n := range nc.Orchestrator.connectedNodes {
			if n.Conn.GetID() == data.NodeID && n != nc {
				go n.Conn.Close()
				nc.Orchestrator.connectedNodes = append(nc.Orchestrator.connectedNodes[:i], nc.Orchestrator.connectedNodes[i+1:]...)
				break
			}
		}
	}

	// after this we have sucessfully established a session
	resp := &dshardorchestrator.IdentifiedData{
		NodeID:      nc.Conn.GetID(),
		TotalShards: totalShards,
	}

	go nc.Conn.SendLogErr(dshardorchestrator.EvtIdentified, resp)

	nc.mu.Lock()
	nc.version = data.Version
	nc.sessionEstablished = true
	nc.runningShards = data.RunningShards
	nc.mu.Unlock()

	nc.Conn.Log(dshardorchestrator.LogInfo, nil, fmt.Sprintf("v%s - tot.shards: %d - running.shards: %v", data.Version, data.TotalShards, data.RunningShards))
}

// GetFullStatus returns the current status of the node
func (nc *NodeConn) GetFullStatus() *NodeStatus {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	status := &NodeStatus{
		ID:                 nc.Conn.GetID(),
		SessionEstablished: nc.sessionEstablished,
		MigratingShard:     nc.shardMigrationShard,
		Connected:          nc.connected,
		DisconnectedAt:     nc.disconnectedAt,
		Version:            nc.version,
	}

	status.Shards = make([]int, len(nc.runningShards))
	copy(status.Shards, nc.runningShards)

	if nc.shardMigrationMode == dshardorchestrator.ShardMigrationModeFrom {
		status.MigratingTo = nc.shardMigrationOtherNodeID
	} else if nc.shardMigrationMode == dshardorchestrator.ShardMigrationModeTo {
		status.MigratingFrom = nc.shardMigrationOtherNodeID
	}

	return status
}

func (nc *NodeConn) StartShard(shard int) {
	go nc.Conn.SendLogErr(dshardorchestrator.EvtStartShard, &dshardorchestrator.StartShardData{
		ShardID: shard,
	})
}

func (nc *NodeConn) StopShard(shard int) {
	go nc.Conn.SendLogErr(dshardorchestrator.EvtStopShard, &dshardorchestrator.StopShardData{
		ShardID: shard,
	})
}

func (nc *NodeConn) Shutdown() {
	nc.mu.Lock()
	nc.shuttingDown = true
	go nc.Conn.SendLogErr(dshardorchestrator.EvtShutdown, nil)
	nc.mu.Unlock()
	return
}
