package orchestrator

import (
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

func (nid *StdNodeIDProvider) GenerateID() string {
	tns := time.Now().UnixNano()
	c := atomic.AddInt64(nid.Counter, 1)

	return fmt.Sprintf("%d-#%d", tns, c)
}
