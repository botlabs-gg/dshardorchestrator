package dshardorchestrator

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
	"reflect"
)

// The internal event IDs are hardcoded to preserve compatibility between versions
type EventType uint32

const (
	// sent from nodes when they connect to establish a session
	EvtIdentify EventType = 1
	// sent by the orchestrator in response to identify to complete the session establishment
	EvtIdentified EventType = 2

	EvtStartShard EventType = 3
	EvtStopShard  EventType = 4
	EvtShutdown   EventType = 5

	EvtPrepareShardmigration       EventType = 6
	EvtStartShardMigration         EventType = 7
	EvtShardMigrationDataProcessed EventType = 8
	EvtAllShardMigrationDataSent   EventType = 9
	EvtAllUserdataSent             EventType = 10

	// This isn't an event per se, but this marks where user id's start
	// events with higher ID than this are registered and fully handled by implementations of the node interface
	// and will not be decoded or touched by the orchestrator.
	//
	// This can be used to transfer any kind of data during shard migration from the old node to the new node
	// to do that you could register a new event for "Guild" states, and send those over one by one.
	EvtShardMigrationDataStartID EventType = 100
)

var EventsToStringMap = map[EventType]string{
	1: "Identify",
	2: "Identified",

	3: "StartShard",
	4: "StopShard",
	5: "Shutdown",

	6:  "PrepareShardmigration",
	7:  "StartShardMigration",
	8:  "ShardMigrationDataProcessed",
	9:  "AllShardMigrationDataSent",
	10: "AllUserdataSent",
}

func (evt EventType) String() string {
	return EventsToStringMap[evt]
}

// Mapping of events to structs for their data
var EvtDataMap = map[EventType]interface{}{
	EvtIdentify:              IdentifyData{},
	EvtIdentified:            IdentifiedData{},
	EvtStartShard:            StartShardData{},
	EvtPrepareShardmigration: PrepareShardmigrationData{},
	EvtStartShardMigration:   StartShardData{},
	EvtAllUserdataSent:       AllUSerDataSentData{},
}

// RegisterUserEvent registers a new user event to be used in shard migration for example
// calling this after opening a connection or otherwise concurrently will cause race conditions
// the reccomended way would be to call this in init()
//
// panics if id is less than 100, as that's reserved id's for inernal use
func RegisterUserEvent(name string, id EventType, dataType interface{}) {
	if id < EvtShardMigrationDataStartID {
		panic(errors.New("tried registering user event with event type less than 100"))
	}

	EvtDataMap[id] = dataType
	EventsToStringMap[id] = "UserEvt:" + name
}

type Message struct {
	EvtID EventType

	// only 1 of RawBody or DecodeBody is present, not both
	RawBody     []byte
	DecodedBody interface{}
}

// EncodeMessage is the same as EncodeMessageRaw but also encodes the data passed using msgpack
func EncodeMessage(evtID EventType, data interface{}) ([]byte, error) {
	if data == nil {
		return EncodeMessageRaw(evtID, nil), nil
	}

	if c, ok := data.([]byte); ok {
		return EncodeMessageRaw(evtID, c), nil
	}

	serialized, err := msgpack.Marshal(data)
	if err != nil {
		return nil, errors.WithMessage(err, "msgpack.Marshal")
	}

	return EncodeMessageRaw(evtID, serialized), nil

}

// EncodeMessageRaw encodes the event to the wire format
// The wire format is pretty basic, first 4 bytes is a uin32 representing what type of event this is
// next 4 bytes is another uin32 which represents the length of the body
// next n bytes is the body itself, which can even be empty in some cases
func EncodeMessageRaw(evtID EventType, data []byte) []byte {
	var buf bytes.Buffer

	tmpBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmpBuf, uint32(evtID))
	buf.Write(tmpBuf)

	l := uint32(len(data))
	binary.LittleEndian.PutUint32(tmpBuf, l)
	buf.Write(tmpBuf)
	buf.Write(data)

	return buf.Bytes()
}

type UnknownEventError struct {
	Evt EventType
}

func (uee *UnknownEventError) Error() string {
	return fmt.Sprintf("Unknown event: %d", uee.Evt)
}

func DecodePayload(evtID EventType, payload []byte) (interface{}, error) {
	t, ok := EvtDataMap[evtID]

	if !ok {
		return nil, &UnknownEventError{Evt: evtID}
	}

	if t == nil {
		return nil, nil
	}

	clone := reflect.New(reflect.TypeOf(t)).Interface()
	err := msgpack.Unmarshal(payload, clone)
	return clone, err
}
