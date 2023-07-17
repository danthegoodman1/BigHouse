package temporal

import (
	"context"
	"fmt"
	"github.com/danthegoodman1/BigHouse/fly"
	"github.com/danthegoodman1/BigHouse/utils"
	"github.com/rs/zerolog"
	"github.com/samber/lo"
	"go.temporal.io/sdk/workflow"
	"time"
)

var (
	QueryExecutorPrefix = "wf_sample"
)

type (
	QueryExecutorActivities struct{}

	QueryExecutorInput struct {
		NumNodes int
		Query    string
	}
	QueryExecutorOutput struct {
		Result string
	}
)

func QueryExecutor(ctx workflow.Context, input QueryExecutorInput) (*QueryExecutorOutput, error) {
	logger := GetLogger(ctx)
	logger.Debug().Msg("starting query executor")

	var ac *QueryExecutorActivities

	// Get keeper info
	keeperInfo, err := execLocalActivityIO(ctx, ac.GetKeeperInfo, GetKeeperInfoIn{}, time.Second*5)
	if err != nil {
		return nil, fmt.Errorf("error in GetKeeperInfo: %w", err)
	}

	// Create nodes and boostrap the cluster
	createdNodes, err := execLocalActivityIO(ctx, ac.SpawnNodes, SpawnNodesInput{
		NumNodes:   3,
		Timeout:    time.Second * 15,
		KeeperHost: keeperInfo.KeeperURL,
		Cluster:    keeperInfo.Cluster,
	}, time.Second*6)
	if err != nil {
		return nil, fmt.Errorf("error in SpawnNodes: %w", err)
	}

	logger.Debug().Interface("createdNodes", createdNodes).Msg("created nodes")

	// TODO: Execute query on cluster, upload results to s3
	// TODO: launch child workflow to clean up nodes?
	// TODO: Return S3 url

	return nil, nil
}

type (
	GetKeeperInfoIn struct {
	}
	KeeperInfo struct {
		KeeperURL, Cluster string
	}
)

func (ac *QueryExecutorActivities) GetKeeperInfo(ctx context.Context, input GetKeeperInfoIn) (*KeeperInfo, error) {
	logger := zerolog.Ctx(ctx)
	logger.Debug().Msg("getting keeper info")
	return &KeeperInfo{
		KeeperURL: "287436db300158.vm.test-bighouse-keeper.internal",
		Cluster:   utils.GenRandomAlpha(""),
	}, nil
}

type (
	SpawnNodesInput struct {
		NumNodes            int
		Timeout             time.Duration
		KeeperHost, Cluster string
	}
	SpawnedNodes struct {
		Machines []*fly.FlyMachine
	}

	AsyncFlyMachine struct {
		Err     error
		Machine *fly.FlyMachine
	}
)

func (ac *QueryExecutorActivities) SpawnNodes(ctx context.Context, input SpawnNodesInput) (*SpawnedNodes, error) {
	logger := zerolog.Ctx(ctx)
	rc := make(chan AsyncFlyMachine, input.NumNodes)
	namePrefix := utils.GenRandomAlpha("")
	tc, cancel := context.WithTimeout(ctx, input.Timeout)
	defer cancel()

	var responses []AsyncFlyMachine

	remoteReplicas := ""
	for i := 0; i < input.NumNodes; i++ {
		nodeName := fmt.Sprintf("%s-%d", namePrefix, i)
		remoteReplicas += fmt.Sprintf("<replica><host>%s.name.kv._metadata.%s.internal</host><port>9000</port></replica>", nodeName, utils.FLY_APP)
	}

	shard := utils.GenRandomShortID()

	// Create machines
	for i := 0; i < input.NumNodes; i++ {
		go func(ctx context.Context, c chan AsyncFlyMachine, i int) {
			nodeName := fmt.Sprintf("%s-%d", namePrefix, i)
			machine, err := fly.CreateFullCHMachine(ctx, nodeName, input.KeeperHost, "2181", remoteReplicas, shard, input.Cluster, nodeName)
			if err != nil {
				logger.Error().Err(err).Int("index", i).Msg("error spawning fly machine")
			}
			c <- AsyncFlyMachine{
				Err:     err,
				Machine: machine,
			}
		}(tc, rc, i)
	}

	// Collect responses
	for i := 0; i < input.NumNodes; i++ {
		res := <-rc
		responses = append(responses, res)
	}

	// TODO: check if % of responses are within limit for still doing the query
	readyMachines := lo.FilterMap(responses, func(item AsyncFlyMachine, index int) (*fly.FlyMachine, bool) {
		return item.Machine, item.Err == nil
	})
	logger.Debug().Msgf("%d/%d nodes created", len(readyMachines), input.NumNodes)

	// TODO: Keep pinging nodes until they are ready

	return &SpawnedNodes{Machines: readyMachines}, nil
}
