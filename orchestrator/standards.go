package orchestrator

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/jonas747/discordgo"
	"sync/atomic"
	"time"
)

// StdShardCountProvider is a standard implementation of RecommendedShardCountProvider
type StdShardCountProvider struct {
	DiscordSession *discordgo.Session
}

func (sc *StdShardCountProvider) GetTotalShardCount() (int, error) {
	gwBot, err := sc.DiscordSession.GatewayBot()
	if err != nil {
		return 0, err
	}

	return gwBot.Shards, nil
}

// StdNodeIDProvider is a standard implementation of NodeIDProvider
type StdNodeIDProvider struct {
	Counter *int64
}

func NewNodeIDProvider() *StdNodeIDProvider {
	return &StdNodeIDProvider{
		Counter: new(int64),
	}
}

func (nid *StdNodeIDProvider) GenerateID() string {
	b := make([]byte, 8)
	tns := time.Now().UnixNano()
	binary.LittleEndian.PutUint64(b, uint64(tns))

	c := atomic.AddInt64(nid.Counter, 1)

	tb64 := base64.URLEncoding.EncodeToString(b)

	return fmt.Sprintf("%s-%d", tb64, c)
}
