package dshardorchestrator

type IdentifyData struct {
	TotalShards   int
	RunningShards []int
	Version       string
	NodeID        string
}

type IdentifiedData struct {
	TotalShards int
	NodeID      string
}

type StartShardData struct {
	ShardID int
}

type StopShardData struct {
	ShardID int
}

type PrepareShardmigrationData struct {
	// wether this is the node were migrating the shard from
	Origin  bool
	ShardID int

	SessionID string
	Sequence  int64
}

type StartshardMigrationData struct {
	ShardID int
}

type AllUserDataSentData struct {
	NumEvents int
}
