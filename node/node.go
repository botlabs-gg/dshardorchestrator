package node

import (
	"github.com/jonas747/dshardorchestrator"
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	processingUserdata = new(int64)
)

// Conn is a wrapper around master.Conn, and represents a connection to the master
type Conn struct {
	baseConn *dshardorchestrator.Conn

	bot                 Interface
	orchestratorAddress string

	mu sync.Mutex

	nodeVersion string
	totalShards int
	nodeShards  []int

	reconnecting bool
	sendQueue    [][]byte

	shardMigrationInProgress    bool
	shardMigrationShard         bool
	shardMigrationMode          dshardorchestrator.ShardMigrationMode
	processedUserEvents         int
	shardmigrationTotalUserEvts int
}

// ConnectToOrchestrator attempts to connect to master ,if it fails it will launch a reconnect loop and wait until the master appears
func ConnectToOrchestrator(bot Interface, addr string, nodeVersion string) (*Conn, error) {
	conn := &Conn{
		bot:                 bot,
		orchestratorAddress: addr,
		nodeVersion:         nodeVersion,
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
	c.baseConn = dshardorchestrator.ConnFromNetCon(netConn)
	c.baseConn.MessageHandler = c.handleMessage
	c.baseConn.ConnClosedHanlder = c.onClosedConn
	go c.baseConn.Listen()

	go c.baseConn.SendLogErr(dshardorchestrator.EvtIdentify, &dshardorchestrator.IdentifyData{
		NodeID:        c.baseConn.GetID(),
		RunningShards: c.nodeShards,
		TotalShards:   c.totalShards,
		Version:       c.nodeVersion,
	})

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
		go c.handleStartShardMigration()
	case dshardorchestrator.EvtAllUserdataSent:
		c.handleAllUserdataSent()
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
}

func (c *Conn) handleStartShard(data *dshardorchestrator.StartShardData) {
	c.bot.StartShard(data.ShardID, "", 0)
	go c.baseConn.SendLogErr(dshardorchestrator.EvtStopShard, data)
}

func (c *Conn) handleStopShard(data *dshardorchestrator.StopShardData) {
	c.bot.StopShard(data.ShardID)
	go c.baseConn.SendLogErr(dshardorchestrator.EvtStopShard, data)
}

func (c *Conn) handlePrepareShardMigration(data *dshardorchestrator.PrepareShardmigrationData) {
	if data.Origin {
		session, seq := c.bot.InitializeShardTransferFrom(data.ShardID)
		data.SessionID = session
		data.Sequence = seq
		go c.baseConn.SendLogErr(dshardorchestrator.EvtPrepareShardmigration, data)
	} else {
		c.bot.InitializeShardTransferTo(data.ShardID, data.SessionID, data.Sequence)
		go c.baseConn.SendLogErr(dshardorchestrator.EvtPrepareShardmigration, data)
	}
}

func (c *Conn) handleStartShardMigration(data *dshardorchestrator.StartshardMigrationData) {
	n := c.bot.StartShardTransferFrom(data.ShardID)
	c.baseConn.SendLogErr(dshardorchestrator.EvtAllUserdataSent, &dshardorchestrator.AllUSerDataSentData{
		NumEvents: n,
	})
}

func (c *Conn) handleAllUserdataSent(data *dshardorchestrator.AllUSerDataSentData) {
	c.mu.Lock()
	c.shardmigrationTotalUserEvts = data.NumEvents
	if data.NumEvents <= c.processedUserEvents {
		c.finishShardMigration()
	}
	c.mu.Unlock()
}

func (c *Conn) handleUserEvt(msg *dshardorchestrator.Message) {
	decoded, err := dshardorchestrator.DecodePayload(msg.EvtID, msg.RawBody)
	if err != nil {
		// TODO
		logrus.WithError(err).Error("failed deocding payload for user event")
		return
	}

	c.bot.HandleUserEvent(msg.EvtID, decoded)

	c.mu.Lock()
	c.processedUserEvents++
	if c.shardmigrationTotalUserEvts > -1 && c.processedUserEvents >= c.shardmigrationTotalUserEvts {
		c.finishShardMigration()
	}
	c.mu.Unlock()
}

func (c *Conn) finishShardMigration() {
	// TODO
}

func (c *Conn) handleShutdown() {
	c.bot.Shutdown()
}

// func (c *Conn) handleGuildState(body []byte) {
// 	defer func() {
// 		atomic.AddInt64(processingUserdata, -1)
// 	}()

// 	var dest master.GuildStateData
// 	err := msgpack.Unmarshal(body, &dest)
// 	if err != nil {
// 		logrus.WithError(err).Error("Failed decoding guildstate")
// 	}

// 	c.bot.LoadGuildState(&dest)
// }

// func (c *Conn) handleResume(data *master.ResumeShardData) {
// 	logrus.Println("Got resume event")

// 	// Wait for remaining guild states to be loaded before we resume, since they're handled concurrently
// 	for atomic.LoadInt64(processingUserdata) > 0 {
// 		runtime.Gosched()
// 	}

// 	c.bot.StartShard(data.Shard, data.SessionID, data.Sequence)
// 	time.Sleep(time.Second)
// 	c.SendLogErr(master.EvtResume, data, true)
// }

// Send sends the message to the master, if the connection is closed it will queue the message if queueFailed is set
func (c *Conn) Send(evtID master.EventType, body interface{}, queueFailed bool) error {
	encoded, err := master.EncodeEvent(evtID, body)
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

func (c *Conn) SendLogErr(evtID master.EventType, body interface{}, queueFailed bool) {
	err := c.Send(evtID, body, queueFailed)
	if err != nil {
		logrus.WithError(err).Error("[SLAVE] Failed sending message to master")
	}
}
