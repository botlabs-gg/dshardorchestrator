package orchestrator

import (
	"fmt"
	"github.com/jonas747/discordgo"
	"github.com/jonas747/dshardorchestrator"
	"github.com/pkg/errors"
	"net"
	"sync"
	"time"
)

// NodeIDProvider is responsible for generating unique ids for nodes
// note that this has to be unique over restarts (a simple in memory counter isn't enough)
type NodeIDProvider interface {
	GenerateID() string
}

// RecommendTotalShardCountProvider will only be called when a fresh new shard count is needed
// this is only the case if: no nodes were running with any shards 10 seconds after the orchestrator starts up
//
// if new nodes without shards connect during this period, they will be put on hold while waiting for nodes with
// running shards to re-identify with the orchestrator, thereby keeping the total shard count across restarts of the orchestrator
// in a fairly realiable manner
//
// but using this interface you can implement completely fine grained control (say storing the shard count in persistent store and updating it manually)
type RecommendTotalShardCountProvider interface {
	GetTotalShardCount() (int, error)
}

// NodeLauncher is responsible for the logic of spinning up new processes
type NodeLauncher interface {
	LaunchNewNode() error
}

type Orchestrator struct {
	// these fields are only safe to edit before you start the orchestrator
	// if you decide to change anything afterwards, it may panic or cause undefined behaviour

	NodeIDProvider     NodeIDProvider
	ShardCountProvider RecommendTotalShardCountProvider
	NodeLauncher       NodeLauncher
	Logger             dshardorchestrator.Logger

	// if set, the orchestrator will make sure that all the shards are always running
	EnsureAllShardsRunning bool

	// the max amount of downtime for a node before we consider it dead and it will start a new node for those shards
	// if set to below zero then it will not perform the restart at all
	MaxNodeDowntimeBeforeRestart time.Duration

	// the maximum amount of shards per node, note that this is solely for the automated tasks the orchestrator provides
	// and you can still go over it if you manually start shards on a node
	MaxShardsPerNode int

	// below fields are protected by the following mutex
	mu             sync.Mutex
	connectedNodes []*NodeConn
	totalShards    int

	activeMigrationFrom string
	activeMigrationTo   string
	netListener         net.Listener
}

func NewStandardOrchestrator(session *discordgo.Session) *Orchestrator {
	return &Orchestrator{
		NodeIDProvider:     &StdNodeIDProvider{},
		ShardCountProvider: &StdShardCountProvider{DiscordSession: session},
	}
}

func (o *Orchestrator) Start(listenAddr string) error {
	err := o.openListen(listenAddr)
	if err != nil {
		return err
	}

	return nil
}

func (o *Orchestrator) Stop() {
	o.netListener.Close()
}

// openListen starts listening for slave connections on the specified address
func (o *Orchestrator) openListen(addr string) error {

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.WithMessage(err, "net.Listen")
	}

	o.netListener = listener

	// go monitorSlaves()
	go o.listenForNodes(listener)

	return nil
}

func (o *Orchestrator) listenForNodes(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			o.Log(dshardorchestrator.LogError, err, "failed accepting incmoing connection")
			break
		}

		o.Log(dshardorchestrator.LogInfo, nil, "new node connection!")
		client := o.NewNodeConn(conn)

		o.mu.Lock()
		o.connectedNodes = append(o.connectedNodes, client)
		o.mu.Unlock()

		go client.listen()
	}
}

// func monitorSlaves() {
// 	ticker := time.NewTicker(time.Second)
// 	lastTimeSawSlave := time.Now()

// 	for {
// 		<-ticker.C
// 		mu.Lock()
// 		if mainSlave != nil {
// 			lastTimeSawSlave = time.Now()
// 		}
// 		mu.Unlock()

// 		if time.Since(lastTimeSawSlave) > time.Second*15 {
// 			logrus.Println("Haven't seen a slave in 15 seconds, starting a new one now")
// 			go StartSlave()
// 			lastTimeSawSlave = time.Now()
// 		}
// 	}
// }

func (o *Orchestrator) FindNodeByID(id string) *NodeConn {
	o.mu.Lock()
	for _, v := range o.connectedNodes {
		if v.Conn.GetID() == id {
			o.mu.Unlock()
			return v
		}
	}
	o.mu.Unlock()

	return nil
}

type NodeStatus struct {
	ID                 string
	SessionEstablished bool
	Shards             []int

	MigratingFrom  string
	MigratingTo    string
	MigratingShard int
}

func (o *Orchestrator) GetFullNodesStatus() []*NodeStatus {
	result := make([]*NodeStatus, 0)

	o.mu.Lock()
	for _, v := range o.connectedNodes {
		o.mu.Unlock()
		result = append(result, v.GetFullStatus())
		o.mu.Lock()
	}
	o.mu.Unlock()

	return result
}

var (
	ErrUnknownFromNode         = errors.New("unknown 'from' node")
	ErrUnknownToNode           = errors.New("unknown 'to' node")
	ErrFromNodeNotRunningShard = errors.New("'from' node not running shard")
	ErrNodeBusy                = errors.New("node is busy")
)

// StartShardMigration attempts to start a shard migration, moving shardID from a origin node to a destination node
func (o *Orchestrator) StartShardMigration(fromNodeID, toNodeID string, shardID int) error {
	fromNode := o.FindNodeByID(fromNodeID)
	toNode := o.FindNodeByID(toNodeID)

	if fromNode == nil {
		return ErrUnknownFromNode
	}

	if toNode == nil {
		return ErrUnknownToNode
	}

	// mark the origin node as busy
	fromNode.mu.Lock()
	// make sure the node were migrating from actually holds the shard
	foundShard := false
	for _, v := range fromNode.runningShards {
		if v == shardID {
			foundShard = true
			break
		}
	}

	// it did not hold the shard
	if !foundShard {
		fromNode.mu.Unlock()
		return ErrFromNodeNotRunningShard
	}

	// make sure its not busy
	if fromNode.shardMigrationMode != dshardorchestrator.ShardMigrationModeNone {
		fromNode.mu.Unlock()
		return ErrNodeBusy
	}

	fromNode.shardMigrationMode = dshardorchestrator.ShardMigrationModeFrom
	fromNode.shardMigrationOtherNodeID = toNodeID
	fromNode.shardMigrationShard = shardID
	// fromNode.shardmigrationTotalUserEvts = 0
	fromNode.mu.Unlock()

	// mark the destination node as busy
	toNode.mu.Lock()

	// make sure its not busy
	if toNode.shardMigrationMode != dshardorchestrator.ShardMigrationModeNone {
		toNode.mu.Unlock()

		fromNode.mu.Lock()
		// need to rollback the from state as we cannot go further
		fromNode.shardMigrationMode = dshardorchestrator.ShardMigrationModeNone
		fromNode.shardMigrationOtherNodeID = ""
		fromNode.shardMigrationShard = -1
		fromNode.mu.Unlock()

		toNode.mu.Unlock()
		return ErrNodeBusy
	}

	toNode.shardMigrationMode = dshardorchestrator.ShardMigrationModeTo
	toNode.shardMigrationOtherNodeID = fromNodeID
	toNode.shardMigrationShard = shardID
	toNode.mu.Unlock()

	o.Log(dshardorchestrator.LogInfo, nil, fmt.Sprintf("migrating shard %d from %q to %q", shardID, fromNodeID, toNodeID))

	// everything passed, we can start the migration of the shard
	fromNode.Conn.SendLogErr(dshardorchestrator.EvtPrepareShardmigration, &dshardorchestrator.PrepareShardmigrationData{
		Origin:  true,
		ShardID: shardID,
	})

	return nil
}

func (o *Orchestrator) Log(level dshardorchestrator.LogLevel, err error, msg string) {
	if err != nil {
		msg = msg + ": " + err.Error()
	}

	if o.Logger == nil {
		dshardorchestrator.StdLogInstance.Log(level, msg)
	} else {
		o.Logger.Log(level, msg)
	}
}
