package orchestrator

import (
	"fmt"
	"github.com/jonas747/dshardorchestrator"
	"time"
)

type monitor struct {
	orchestrator *Orchestrator

	started  time.Time
	stopChan chan bool

	lastTimeLaunchedNode time.Time
	lastTimeStartedShard time.Time
	shardsLastSeenTimes  []time.Time
}

func (mon *monitor) run() {
	mon.started = time.Now()
	ticker := time.NewTicker(time.Second)

	// allow timem for shards to connect before we start launching stuff
	time.Sleep(time.Second * 10)

	for {
		select {
		case <-ticker.C:
			mon.tick()
		case <-mon.stopChan:
			return
		}
	}
}

func (mon *monitor) stop() {
	close(mon.stopChan)
}

func (mon *monitor) ensureTotalShards() int {
	mon.orchestrator.mu.Lock()
	defer mon.orchestrator.mu.Unlock()

	// totalshards has been set already
	totalShards := mon.orchestrator.totalShards
	if totalShards != 0 {
		return totalShards
	}

	// has not been set, try to set it
	totalShards, err := mon.orchestrator.ShardCountProvider.GetTotalShardCount()
	if err != nil {
		mon.orchestrator.Log(dshardorchestrator.LogError, err, "monitor: failed fetching total shard count, retrying in a second")
		return 0
	}

	if totalShards == 0 {
		mon.orchestrator.Log(dshardorchestrator.LogError, err, "monitor: ShardCountProvider returned 0 without error, retrying in a second")
		return 0
	}

	// successfully set it
	mon.orchestrator.totalShards = totalShards
	mon.orchestrator.Log(dshardorchestrator.LogInfo, nil, fmt.Sprintf("monitor: set total shard count to %d", totalShards))
	return totalShards
}

func (mon *monitor) tick() {
	if !mon.orchestrator.EnsureAllShardsRunning {
		// currently this is the only purpose of the monitor, it may be extended to perform more as it could be a reliable way of handling a bunch of things
		return
	}

	totalShards := mon.ensureTotalShards()
	if totalShards == 0 {
		return
	}

	if time.Since(mon.lastTimeStartedShard) < time.Second*5 {
		return
	}

	if mon.shardsLastSeenTimes == nil {
		mon.shardsLastSeenTimes = make([]time.Time, totalShards)
		for i, _ := range mon.shardsLastSeenTimes {
			mon.shardsLastSeenTimes[i] = time.Now()
		}
	}

	runningShards := make([]bool, totalShards)

	// find the disconnect times for all shards that has disconnected
	fullNodeStatuses := mon.orchestrator.GetFullNodesStatus()
	for _, ns := range fullNodeStatuses {
		for _, s := range ns.Shards {
			if !ns.Connected {
				continue
			}

			if ns.Connected {
				runningShards[s] = true
				mon.shardsLastSeenTimes[s] = time.Now()
			}
		}
	}

	// find out which shards to start
	shardsToStart := make([]int, 0)

OUTER:
	for i, lastTimeConnected := range mon.shardsLastSeenTimes {
		if runningShards[i] || time.Since(lastTimeConnected) < mon.orchestrator.MaxNodeDowntimeBeforeRestart {
			continue
		}

		// check if this shard is in a shard migration, in which case ignore it
		for _, ns := range fullNodeStatuses {
			if ns.MigratingShard == i && (ns.MigratingFrom != "" || ns.MigratingTo != "") {
				continue OUTER
			}
		}

		shardsToStart = append(shardsToStart, i)
	}

	if len(shardsToStart) < 1 {
		return
	}

	mon.orchestrator.Log(dshardorchestrator.LogInfo, nil, fmt.Sprintf("monitor: need to start %d shards...", len(shardsToStart)))

	// start one
	// reason we don't start them all at once is that there's no real point in that, you can only start a shard every 5 seconds anyways
	for _, v := range fullNodeStatuses {
		if !v.Connected || !v.SessionEstablished {
			continue
		}

		if len(v.Shards) < mon.orchestrator.MaxShardsPerNode {
			err := mon.orchestrator.StartShard(v.ID, shardsToStart[0])
			if err != nil {
				mon.orchestrator.Log(dshardorchestrator.LogError, err, "monitor: failed starting shard")
			}
			mon.lastTimeStartedShard = time.Now()
			return
		}
	}

	// if we got here that means that there's no more nodes available, so start one
	if time.Since(mon.lastTimeLaunchedNode) < time.Second*5 {
		// allow 5 seconds wait time in between each node launch
		mon.orchestrator.Log(dshardorchestrator.LogDebug, nil, "monitor: can't start new node, on cooldown")
		return
	}

	if mon.orchestrator.NodeLauncher == nil {
		mon.orchestrator.Log(dshardorchestrator.LogError, nil, "monitor: can't start new node, no node launcher set...")
		return
	}

	err := mon.orchestrator.NodeLauncher.LaunchNewNode()
	if err != nil {
		mon.orchestrator.Log(dshardorchestrator.LogError, err, "monitor: failed starting a new node")
		return
	}

	mon.lastTimeLaunchedNode = time.Now()
}
