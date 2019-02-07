package orchestrator

import (
	"bufio"
	"fmt"
	"github.com/jonas747/discordgo"
	"io"
	"os"
	"os/exec"
	"strconv"
	"sync"
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

type IDGenerator interface {
	GenerateID() (string, error)
}

type StdNodeLauncher struct {
	IDGenerator IDGenerator
	CmdName     string
	Args        []string

	mu                   sync.Mutex
	lastTimeLaunchedNode time.Time
}

func NewNodeLauncher(cmdName string, args []string, idGen IDGenerator) NodeLauncher {
	return &StdNodeLauncher{
		IDGenerator: idGen,
		CmdName:     cmdName,
		Args:        args,
	}
}

func (nl *StdNodeLauncher) LaunchNewNode() (string, error) {
	// ensure were not starting nodes too fast since the id generation only does millisecond unique ids
	nl.mu.Lock()
	if time.Since(nl.lastTimeLaunchedNode) < time.Millisecond*100 {
		time.Sleep(time.Millisecond * 100)
	}
	nl.lastTimeLaunchedNode = time.Now()
	nl.mu.Unlock()

	// generate the node id
	var err error
	id := ""
	if nl.IDGenerator != nil {
		id, err = nl.IDGenerator.GenerateID()
	} else {
		id = nl.GenerateID()
	}

	if err != nil {
		return "", err
	}

	args := append(nl.Args, "-nodeid", id)

	// launch it
	cmd := exec.Command(nl.CmdName, args...)
	cmd.Env = os.Environ()

	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	cmd.Dir = wd

	stdOut, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	go nl.PrintOutput(stdOut)

	stdErr, err := cmd.StderrPipe()
	if err != nil {
		return "", err
	}
	go nl.PrintOutput(stdErr)

	err = cmd.Start()
	return id, err
}

func (nl *StdNodeLauncher) GenerateID() string {
	tms := time.Now().UnixNano() / 1000000
	st := strconv.FormatInt(tms, 36)

	pid := os.Getpid()
	host, _ := os.Hostname()

	return fmt.Sprintf("%s-%d-%s", host, pid, st)
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
