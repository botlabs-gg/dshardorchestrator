package node

import (
	"github.com/jonas747/dshardorchestrator"
)

type SessionInfo struct {
	TotalShards int
}

type Interface interface {
	SessionEstablished(info SessionInfo)

	StopShard(shard int) (sessionID string, sequence int64)
	StartShard(shard int, sessionID string, sequence int64)

	// Caled when the bot should shut down, make sure to send EvtShutdown when completed
	Shutdown()

	InitializeShardTransferFrom(shard int) (sessionID string, sequence int64)
	InitializeShardTransferTo(shard int, sessionID string, sequence int64)

	// this should return when all user events has been sent, with the number of user events sent
	StartShardTransferFrom(shard int) (numEventsSent int)

	HandleUserEvent(evt dshardorchestrator.EventType, data interface{})
}
