package orchestrator

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/jonas747/discordgo"
	"io"
	"os"
	"os/exec"
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

type StdNodeLauncher struct {
	CmdName string
	Args    []string
}

func (nl *StdNodeLauncher) LaunchNewNode() error {
	cmd := exec.Command(nl.CmdName, nl.Args...)
	cmd.Env = os.Environ()

	wd, err := os.Getwd()
	if err != nil {
		return err
	}

	cmd.Dir = wd

	stdOut, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	go nl.PrintOutput(stdOut)

	stdErr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	go nl.PrintOutput(stdErr)

	err = cmd.Start()
	return err
}

func (nl *StdNodeLauncher) PrintOutput(reader io.Reader) {
	breader := bufio.NewReader(reader)
	for {
		s, err := breader.ReadString('\n')
		if len(s) > 0 {
			s = s[:len(s)-1]
		}

		fmt.Println("NODE: " + s)

		if err != nil {
			break
		}
	}
}
