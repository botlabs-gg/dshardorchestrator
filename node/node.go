package node

import (
	"github.com/jonas747/dshardorchestrator"
	"net"
	"sync"
	"time"
)

var (
	processingUserdata = new(int64)
)

// Conn represents a connection to the orchestrator
type Conn struct {
	baseConn *dshardorchestrator.Conn

	bot                 Interface
	orchestratorAddress string
	logger              dshardorchestrator.Logger

	// below fields are protected by the mutex
	mu sync.Mutex

	nodeVersion string
	totalShards int
	nodeShards  []int

	reconnecting bool
	sendQueue    [][]byte

	shardMigrationMode          dshardorchestrator.ShardMigrationMode
	shardMigrationShard         int
	processedUserEvents         int
	shardmigrationTotalUserEvts int

	// gateway settings
	discordSessionID string
	discordSequence  int64
}

// ConnectToOrchestrator attempts to connect to master ,if it fails it will launch a reconnect loop and wait until the master appears
func ConnectToOrchestrator(bot Interface, addr string, nodeVersion string, logger dshardorchestrator.Logger) (*Conn, error) {
	conn := &Conn{
		bot:                 bot,
		orchestratorAddress: addr,
		nodeVersion:         nodeVersion,
		logger:              logger,
	}

	go conn.reconnectLoop(false)

	return conn, nil
}

func (c *Conn) connect() error {
	netConn, err := net.Dial("tcp", c.orchestratorAddress)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.baseConn = dshardorchestrator.ConnFromNetCon(netConn, c.logger)
	c.baseConn.MessageHandler = c.handleMessage
	c.baseConn.ConnClosedHanlder = c.onClosedConn
	c.reconnecting = false
	go c.baseConn.Listen()

	go c.SendLogErr(dshardorchestrator.EvtIdentify, &dshardorchestrator.IdentifyData{
		NodeID:        c.baseConn.GetID(),
		RunningShards: c.nodeShards,
		TotalShards:   c.totalShards,
		Version:       c.nodeVersion,
	}, false)

	c.baseConn.Log(dshardorchestrator.LogInfo, nil, "sent identify")
	c.mu.Unlock()

	return nil
}

func (c *Conn) onClosedConn() {
	go c.reconnectLoop(true)
}

func (c *Conn) reconnectLoop(running bool) {
	c.mu.Lock()
	if c.reconnecting {
		c.mu.Unlock()
		return
	}

	c.reconnecting = true
	c.mu.Unlock()

	go func() {
		for {
			if c.tryReconnect(running) {
				break
			}

			time.Sleep(time.Second * 5)
		}
	}()
}

func (c *Conn) tryReconnect(running bool) bool {
	err := c.connect()
	if err != nil {
		return false
	}

	return true
}

func (c *Conn) handleMessage(m *dshardorchestrator.Message) {
	switch m.EvtID {
	case dshardorchestrator.EvtIdentified:
		c.handleIdentified(m.DecodedBody.(*dshardorchestrator.IdentifiedData))
	case dshardorchestrator.EvtStartShard:
		c.handleStartShard(m.DecodedBody.(*dshardorchestrator.StartShardData))
	case dshardorchestrator.EvtStopShard:
		c.handleStopShard(m.DecodedBody.(*dshardorchestrator.StopShardData))
	case dshardorchestrator.EvtShutdown:
		c.handleShutdown()
	case dshardorchestrator.EvtPrepareShardmigration:
		c.handlePrepareShardMigration(m.DecodedBody.(*dshardorchestrator.PrepareShardmigrationData))
	case dshardorchestrator.EvtStartShardMigration:
		go c.handleStartShardMigration(m.DecodedBody.(*dshardorchestrator.StartshardMigrationData))
	case dshardorchestrator.EvtAllUserdataSent:
		c.handleAllUserdataSent(m.DecodedBody.(*dshardorchestrator.AllUSerDataSentData))
	}
}

func (c *Conn) handleIdentified(data *dshardorchestrator.IdentifiedData) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.totalShards = data.TotalShards
	c.baseConn.ID.Store(data.NodeID)
	go c.bot.SessionEstablished(SessionInfo{
		TotalShards: data.TotalShards,
	})

	c.LogLock(dshardorchestrator.LogInfo, nil, "Session established")
}

func (c *Conn) handleStartShard(data *dshardorchestrator.StartShardData) {
	c.bot.StartShard(data.ShardID, "", 0)

	c.mu.Lock()
	c.nodeShards = append(c.nodeShards, data.ShardID)
	c.mu.Unlock()

	go c.SendLogErr(dshardorchestrator.EvtStopShard, data, true)
}

func (c *Conn) handleStopShard(data *dshardorchestrator.StopShardData) {

	c.mu.Lock()
	for i, v := range c.nodeShards {
		if v == data.ShardID {
			c.nodeShards = append(c.nodeShards[:i], c.nodeShards[i+1:]...)
			break
		}
	}
	c.mu.Unlock()

	c.bot.StopShard(data.ShardID)

	go c.SendLogErr(dshardorchestrator.EvtStopShard, data, true)
}

func (c *Conn) handlePrepareShardMigration(data *dshardorchestrator.PrepareShardmigrationData) {
	if data.Origin {
		c.mu.Lock()
		c.shardMigrationMode = dshardorchestrator.ShardMigrationModeFrom
		c.shardMigrationShard = data.ShardID
		c.mu.Unlock()

		session, seq := c.bot.InitializeShardTransferFrom(data.ShardID)
		data.SessionID = session
		data.Sequence = seq
		go c.SendLogErr(dshardorchestrator.EvtPrepareShardmigration, data, true)
	} else {
		c.mu.Lock()
		c.discordSessionID = data.SessionID
		c.discordSequence = data.Sequence

		c.shardMigrationMode = dshardorchestrator.ShardMigrationModeTo
		c.shardMigrationShard = data.ShardID
		c.processedUserEvents = 0
		c.shardmigrationTotalUserEvts = -1
		c.mu.Unlock()

		c.bot.InitializeShardTransferTo(data.ShardID, data.SessionID, data.Sequence)
		go c.SendLogErr(dshardorchestrator.EvtPrepareShardmigration, data, true)
	}
}

func (c *Conn) handleStartShardMigration(data *dshardorchestrator.StartshardMigrationData) {
	n := c.bot.StartShardTransferFrom(data.ShardID)
	go c.SendLogErr(dshardorchestrator.EvtAllUserdataSent, &dshardorchestrator.AllUSerDataSentData{
		NumEvents: n,
	}, true)
}

func (c *Conn) handleAllUserdataSent(data *dshardorchestrator.AllUSerDataSentData) {
	c.mu.Lock()
	c.shardmigrationTotalUserEvts = data.NumEvents
	if data.NumEvents <= c.processedUserEvents {
		c.finishShardMigrationTo()
	}
	c.mu.Unlock()
}

func (c *Conn) handleUserEvt(msg *dshardorchestrator.Message) {
	decoded, err := dshardorchestrator.DecodePayload(msg.EvtID, msg.RawBody)
	if err != nil {
		go c.LogLock(dshardorchestrator.LogError, err, "failed deocding payload for user event, skipping it")
	} else {
		c.bot.HandleUserEvent(msg.EvtID, decoded)
	}

	c.mu.Lock()
	c.processedUserEvents++
	if c.shardmigrationTotalUserEvts > -1 && c.processedUserEvents >= c.shardmigrationTotalUserEvts {
		c.finishShardMigrationTo()
	}
	c.mu.Unlock()
}

func (c *Conn) finishShardMigrationTo() {
	c.mu.Lock()
	go c.bot.StartShard(c.shardMigrationShard, c.discordSessionID, c.discordSequence)

	c.shardMigrationMode = dshardorchestrator.ShardMigrationModeNone
	c.shardMigrationShard = -1
	c.shardmigrationTotalUserEvts = -1
	c.discordSequence = 0
	c.discordSessionID = ""
	c.mu.Unlock()
}

func (c *Conn) handleShutdown() {
	c.bot.Shutdown()
}

// Send sends the message to the master, if the connection is closed it will queue the message if queueFailed is set
func (c *Conn) Send(evtID dshardorchestrator.EventType, body interface{}, queueFailed bool) error {
	encoded, err := dshardorchestrator.EncodeMessage(evtID, body)
	if err != nil {
		return err
	}

	c.mu.Lock()
	if c.reconnecting {
		if queueFailed {
			c.sendQueue = append(c.sendQueue, encoded)
		}
		c.mu.Unlock()
		return nil
	}

	err = c.baseConn.SendNoLock(encoded)

	c.mu.Unlock()

	return err
}

func (c *Conn) SendLogErr(evtID dshardorchestrator.EventType, body interface{}, queueFailed bool) {
	err := c.Send(evtID, body, queueFailed)
	if err != nil {
		c.LogLock(dshardorchestrator.LogError, err, "failed sending message to orchestrator")
	}
}

func (c *Conn) LogLock(level dshardorchestrator.LogLevel, err error, msg string) {
	c.mu.Lock()
	c.baseConn.Log(level, err, msg)
	c.mu.Unlock()
}
