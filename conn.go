package dshardorchestrator

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
)

// Conn represents a connection from either node to the orchestrator or the other way around
// it implements common logic across both sides
type Conn struct {
	netConn net.Conn
	sendmu  sync.Mutex

	ID atomic.Value

	// called on incoming messages
	MessageHandler func(*Message)

	// called when the connection is closed
	ConnClosedHanlder func()
}

// ConnFromNetCon wraos a Conn around a net.Conn
func ConnFromNetCon(conn net.Conn) *Conn {
	c := &Conn{
		netConn: conn,
	}

	c.ID.Store("unknown-" + strconv.FormatInt(getNewID(), 10))
	return c
}

// Listen starts listening for events on the connection
func (c *Conn) Listen() {
	logrus.Info("Master/Slave connection: starting listening for events ", c.GetID())

	var err error
	defer func() {
		if err != nil {
			logrus.WithError(err).Error("An error occured while handling a connection")
		}

		c.netConn.Close()

		if c.ConnClosedHanlder != nil {
			c.ConnClosedHanlder()
		}
	}()

	idBuf := make([]byte, 4)
	lenBuf := make([]byte, 4)
	for {

		// Read the event id
		_, err = c.netConn.Read(idBuf)
		if err != nil {
			logrus.WithError(err).Error("Failed reading event id")
			return
		}

		// Read the body length
		_, err = c.netConn.Read(lenBuf)
		if err != nil {
			logrus.WithError(err).Error("Failed reading event length")
			return
		}

		id := EventType(binary.LittleEndian.Uint32(idBuf))
		l := binary.LittleEndian.Uint32(lenBuf)
		body := make([]byte, int(l))
		if l > 0 {
			// Read the body, if there was one
			_, err = io.ReadFull(c.netConn, body)
			if err != nil {
				logrus.WithError(err).Error("Failed reading body")
				return
			}
		}

		msg := &Message{
			EvtID: id,
		}

		if id < 100 {
			decoded, err := DecodePayload(id, body)
			if err != nil {
				logrus.WithError(err).Error("Failed deocding payload")
			}
			msg.DecodedBody = decoded
		} else {
			msg.RawBody = body
		}

		c.MessageHandler(msg)
	}
}

// Send sends the specified message over the connection, marshaling the data using json
// this locks the writer
func (c *Conn) Send(evtID EventType, data interface{}) error {
	encoded, err := EncodeMessage(evtID, data)
	if err != nil {
		return errors.WithMessage(err, "EncodeEvent")
	}

	c.sendmu.Lock()
	defer c.sendmu.Unlock()

	return c.SendNoLock(encoded)
}

// Same as Send but logs the error (usefull for launching send in new goroutines)
func (c *Conn) SendLogErr(evtID EventType, data interface{}) {
	err := c.Send(evtID, data)
	if err != nil {
		logrus.WithError(err).Error("[MASTER] Failed sending message to slave")
	}
}

// SendNoLock sends the specified message over the connection, marshaling the data using json
// This does no locking and the caller is responsible for making sure its not called in multiple goroutines at the same time
func (c *Conn) SendNoLock(data []byte) error {

	_, err := c.netConn.Write(data)
	return errors.WithMessage(err, "netConn.Write")
}

func (c *Conn) GetID() string {
	return c.ID.Load().(string)
}
