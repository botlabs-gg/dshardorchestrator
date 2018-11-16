package dshardorchestrator

import (
	"log"
)

type ShardMigrationMode int

const (
	ShardMigrationModeNone ShardMigrationMode = iota
	ShardMigrationModeTo
	ShardMigrationModeFrom
)

type LogLevel int

const (
	LogError LogLevel = iota
	LogWarning
	LogInfo
	LogDebug
)

type Logger interface {
	Log(level LogLevel, message string)
}

var StdLogInstance = &StdLogger{Level: LogInfo}

type StdLogger struct {
	Level LogLevel
}

func (stdl *StdLogger) Log(level LogLevel, message string) {
	if stdl.Level < level {
		return
	}

	strLevel := ""
	switch level {
	case LogError:
		strLevel = "ERRO"
	case LogWarning:
		strLevel = "WARN"
	case LogInfo:
		strLevel = "INFO"
	case LogDebug:
		strLevel = "DEBG"
	}

	log.Printf("[%s] %s", strLevel, message)
}
