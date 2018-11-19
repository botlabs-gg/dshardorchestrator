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

	monitor *monitor

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
		NodeIDProvider:     NewNodeIDProvider(),
		ShardCountProvider: &StdShardCountProvider{DiscordSession: session},
	}
}

// Start will start the orchestrator, and start to listen fro clients on the specified address
// IMPORTANT: opening this up to the outer internet is bad because there's no authentication.
func (o *Orchestrator) Start(listenAddr string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	err := o.openListen(listenAddr)
	if err != nil {
		return err
	}

	o.monitor = &monitor{
		orchestrator: o,
		stopChan:     make(chan bool),
	}
	go o.monitor.run()

	return nil
}

// Stop will stop the orchestrator and the monitor
func (o *Orchestrator) Stop() {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.netListener.Close()
	o.monitor.stop()
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
	Version            string
	SessionEstablished bool
	Shards             []int
	Connected          bool
	DisconnectedAt     time.Time

	MigratingFrom  string
	MigratingTo    string
	MigratingShard int
}

// GetFullNodesStatus returns the full status of all nodes
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
func (o *Orchestrator) StartShardMigration(toNodeID string, shardID int) error {

	// find the origin node
	fromNodeID := ""

	status := o.GetFullNodesStatus()
OUTER:
	for _, n := range status {
		if !n.Connected {
			continue
		}

		for _, s := range n.Shards {
			if s == shardID {
				fromNodeID = n.ID
				break OUTER
			}
		}
	}

	toNode := o.FindNodeByID(toNodeID)
	fromNode := o.FindNodeByID(fromNodeID)

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

// Log will log to the designated logger or he standard logger
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

var (
	ErrNoNodeLauncher = errors.New("orchestrator.NodeLauncher is nil")
)

// StartNewNode will launch a new node, it will not wait for it to connect
func (o *Orchestrator) StartNewNode() error {
	if o.NodeLauncher == nil {
		return ErrNoNodeLauncher
	}

	return o.NodeLauncher.LaunchNewNode()
}

var (
	ErrShardAlreadyRunning = errors.New("shard already running")
	ErrUnknownNode         = errors.New("unknown node")
)

// StartShard will start the specified shard on the specified node
// it will return ErrShardAlreadyRunning if the shard is running on another node already
func (o *Orchestrator) StartShard(nodeID string, shard int) error {
	fullStatus := o.GetFullNodesStatus()
	for _, v := range fullStatus {
		if !v.Connected {
			continue
		}

		if dshardorchestrator.ContainsInt(v.Shards, shard) {
			return ErrShardAlreadyRunning
		}
	}

	node := o.FindNodeByID(nodeID)
	if node == nil {
		return ErrUnknownNode
	}

	node.StartShard(shard)
	return nil
}

// StopShard will stop the specified shard on whatever node it's running on, or do nothing if it's not running
func (o *Orchestrator) StopShard(shard int) error {
	fullStatus := o.GetFullNodesStatus()
	for _, v := range fullStatus {
		if !v.Connected {
			continue
		}

		if dshardorchestrator.ContainsInt(v.Shards, shard) {
			// bingo
			node := o.FindNodeByID(v.ID)
			if node == nil {
				return ErrUnknownNode
			}

			node.StopShard(shard)
		}
	}

	return nil
}

// func (o *Orchestrator) FullMigrationToNewNodes() error {
// 	if o.NodeLauncher == nil {
// 		return ErrNoNodeLauncher
// 	}

// 	oldNodes := o.GetFullNodesStatus()

// 	for _, node := range oldNodes {
// 		newNode, err := o.startWaitForNode()
// 		if err != nil {
// 			return err
// 		}

// 	}
// }

// func (o *Orchestrator) startWaitForNode() (string, error) {

// }

// MigrateFullNode migrates all the shards on the origin node to the destination node
// optionally also shutting the origin node down at the end
func (o *Orchestrator) MigrateFullNode(fromNode string, toNodeID string, shutdownOldNode bool) error {
	nodeFrom := o.FindNodeByID(fromNode)
	if nodeFrom == nil {
		return ErrUnknownFromNode
	}

	toNode := o.FindNodeByID(toNodeID)
	if toNode == nil {
		return ErrUnknownToNode
	}

	nodeFrom.mu.Lock()
	shards := make([]int, len(nodeFrom.runningShards))
	copy(shards, nodeFrom.runningShards)
	nodeFrom.mu.Unlock()

	o.Log(dshardorchestrator.LogInfo, nil, fmt.Sprintf("starting full node migration from %s to %s, n-shards: %d", fromNode, toNode, len(shards)))

	for _, s := range shards {
		err := o.StartShardMigration(toNodeID, s)
		if err != nil {
			return err
		}

		// wait for it to be moved before we start the next one
		o.WaitForShardMigration(fromNode, toNodeID, s)

		// wait a bit extra to allow for some time ot catch up on events processing
		time.Sleep(time.Second)
	}

	if shutdownOldNode {
		return o.ShutdownNode(fromNode)
	}

	return nil
}

// ShutdownNode shuts down the specified node
func (o *Orchestrator) ShutdownNode(nodeID string) error {
	node := o.FindNodeByID(nodeID)
	if node == nil {
		return ErrUnknownNode
	}

	node.Shutdown()
	return nil
}

// WaitForShardMigration blocks until a shard migration is complete
func (o *Orchestrator) WaitForShardMigration(fromNodeID string, toNodeID string, shardID int) {
	// wait for the shard to dissapear on the origin node
	for {
		time.Sleep(time.Second)

		fromNode := o.FindNodeByID(fromNodeID)
		if fromNode == nil {
			continue
		}

		fromNode.mu.Lock()
		status := fromNode.GetFullStatus()
		fromNode.mu.Unlock()

		if !dshardorchestrator.ContainsInt(status.Shards, shardID) {
			break
		}
	}

	// wait for it to appear on the new node
	for {
		time.Sleep(time.Millisecond * 100)

		toNode := o.FindNodeByID(toNodeID)
		if toNode == nil {
			continue
		}

		toNode.mu.Lock()
		status := toNode.GetFullStatus()
		toNode.mu.Unlock()

		if dshardorchestrator.ContainsInt(status.Shards, shardID) {
			break
		}
	}

	// AND FINALLY just for safe measure, wait for it to not be in the migrating state
	for {
		time.Sleep(time.Millisecond * 100)

		toNode := o.FindNodeByID(toNodeID)
		if toNode == nil {
			continue
		}

		toNode.mu.Lock()
		status := toNode.GetFullStatus()
		toNode.mu.Unlock()

		if status.MigratingFrom == "" {
			break
		}
	}
}
