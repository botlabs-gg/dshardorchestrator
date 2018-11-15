package dshardorchestrator

type ShardMigrationMode int

const (
	ShardMigrationModeNone ShardMigrationMode = iota
	ShardMigrationModeTo
	ShardMigrationModeFrom
)
