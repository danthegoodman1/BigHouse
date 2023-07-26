package temporal

import (
	"context"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/danthegoodman1/BigHouse/fly"
	"github.com/danthegoodman1/BigHouse/utils"
	"github.com/miekg/dns"
	"github.com/rs/zerolog"
	"github.com/samber/lo"
	"go.temporal.io/sdk/workflow"
	"golang.org/x/sync/errgroup"
	"reflect"
	"strings"
	"sync"
	"time"
)

var (
	QueryExecutorPrefix      = "wf_sample"
	ErrFailedToGetAAAARecord = errors.New("failed to get AAAA record")
)

type (
	QueryExecutorActivities struct{}

	QueryExecutorInput struct {
		Query, Cluster, KeeperHost, CPUKind string
		InitQueries, InitQueriesAll         []string
		DeleteNodes                         bool
		MemoryMB, Cores, NumNodes           int64
	}
	QueryExecutorOutput struct {
		Cols []string
		Rows []any
	}
)

func QueryExecutor(ctx workflow.Context, input QueryExecutorInput) (*QueryExecutorOutput, error) {
	logger := GetLogger(ctx)
	logger.Debug().Msg("starting query executor")

	var ac *QueryExecutorActivities

	// Get keeper info
	// keeperInfo, err := execActivityIO(ctx, ac.GetKeeperInfo, GetKeeperInfoIn{}, time.Second*5)
	// if err != nil {
	// 	return nil, fmt.Errorf("error in GetKeeperInfo: %w", err)
	// }

	// Create nodes and boostrap the cluster
	createdNodes, err := execActivityIO(ctx, ac.SpawnNodes, SpawnNodesInput{
		Timeout:    time.Second * 120,
		KeeperHost: input.KeeperHost,
		Cluster:    input.Cluster,
		CPUKind:    input.CPUKind,
		MemoryMB:   input.MemoryMB,
		Cores:      input.Cores,
		NumNodes:   input.NumNodes,
	}, time.Second*6)
	if err != nil {
		return nil, fmt.Errorf("error in SpawnNodes: %w", err)
	}

	logger.Debug().Msg("created nodes")

	if input.DeleteNodes {
		defer func() {
			// Clean nodes
			err = execActivity(ctx, ac.DeleteNodes, DeleteNodesInput{IDs: lo.Map(createdNodes.Machines, func(item *fly.FlyMachine, index int) string {
				return item.Id
			})}, time.Minute)
			if err != nil {
				logger.Error().Err(err).Msg("error deleting nodes")
			}
		}()
	}

	// Wait for nodes to be ready and query
	logger.Debug().Msg("waiting for ch ready...")
	queryRes, err := execActivityIO(ctx, ac.WaitAndQuery, *&WaitAndQueryInput{
		Machines:    createdNodes.Machines,
		Query:       input.Query,
		InitQueries: input.InitQueries,
	}, time.Minute)
	if err != nil {
		return nil, fmt.Errorf("error in WaitAndQuery: %w", err)
	}

	// TODO: launch child workflow to clean up nodes?
	// TODO: Return S3 url

	return &QueryExecutorOutput{
		Cols: queryRes.Cols,
		Rows: queryRes.Rows,
	}, nil
}

type (
	SpawnNodesInput struct {
		Timeout                      time.Duration
		KeeperHost, Cluster, CPUKind string
		MemoryMB, Cores, NumNodes    int64
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

	var responses []AsyncFlyMachine

	remoteReplicas := ""
	for i := 0; i < int(input.NumNodes); i++ {
		nodeName := fmt.Sprintf("%s-%d", namePrefix, i)
		remoteReplicas += fmt.Sprintf("<replica><host>%s.name.kv._metadata.%s.internal</host><port>9000</port></replica>", nodeName, utils.FLY_APP)
	}

	shard := utils.GenRandomShortID()

	// Create machines
	for i := 0; i < int(input.NumNodes); i++ {
		go func(ctx context.Context, c chan AsyncFlyMachine, i int) {
			nodeName := fmt.Sprintf("%s-%d", namePrefix, i)
			machine, err := fly.CreateFullCHMachine(ctx, nodeName, "ams", input.KeeperHost, "2181", remoteReplicas, shard, input.Cluster, nodeName, input.CPUKind, input.Cores, input.MemoryMB)
			if err != nil {
				logger.Error().Err(err).Int("index", i).Msg("error spawning fly machine")
			}
			c <- AsyncFlyMachine{
				Err:     err,
				Machine: machine,
			}
		}(ctx, rc, i)
	}

	// Collect responses
	for i := 0; i < int(input.NumNodes); i++ {
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

// TODO: optimize input
type (
	WaitAndQueryInput struct {
		Machines                    []*fly.FlyMachine
		Query                       string
		InitQueries, InitQueriesAll []string
	}
	WaitAndQueryOutput struct {
		Runtime time.Duration
		Cols    []string
		Rows    []any
	}
)

func (ac *QueryExecutorActivities) WaitAndQuery(ctx context.Context, input WaitAndQueryInput) (*WaitAndQueryOutput, error) {
	logger := zerolog.Ctx(ctx)
	eg := errgroup.Group{}
	stmt := "select count()+2 from system.zookeeper where path='/clickhouse/task_queue/'"

	outC := make(chan string, len(input.Machines))
	mu := &sync.Mutex{}
	var savedConn driver.Conn
	s := time.Now()
	for _, n := range input.Machines {
		// Keep pinging CH nodes until a good response
		node := n
		eg.Go(func() error {
			nodeAddr := fmt.Sprintf("%s.vm.%s.internal", node.Id, utils.FLY_APP)
			c := dns.Client{}
			m := &dns.Msg{}
			m.SetQuestion(dns.Fqdn(nodeAddr), dns.TypeAAAA)
			m.RecursionDesired = true

			// Keep going until we get an aaaa
			aaaa := ""
			for aaaa == "" && ctx.Err() == nil {
				// for local dev, can probably disable once on fly net? Or can use OS-level DNS with net package
				r, _, err := c.Exchange(m, "[fdaa:2:9366::3]:53")
				if err != nil {
					return fmt.Errorf("error in c.Exchange: %w", err)
				}
				if r.Rcode != dns.RcodeSuccess {
					return ErrFailedToGetAAAARecord
				}

				for _, ain := range r.Answer {
					if a, ok := ain.(*dns.AAAA); ok {
						fmt.Printf("AAAA: %v\n", a.AAAA)
						aaaa = a.AAAA.String()
					}
				}
				time.Sleep(time.Millisecond * 500)
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}

			opts := &clickhouse.Options{
				Addr: []string{fmt.Sprintf("[%s]:9000", aaaa)},
				Auth: clickhouse.Auth{
					Database: "default",
					Username: "default",
				},
				DialTimeout:     time.Second * 5,
				MaxOpenConns:    1,
				MaxIdleConns:    0,
				ConnMaxLifetime: 2 * time.Minute,
				Compression: &clickhouse.Compression{
					Method: clickhouse.CompressionLZ4,
				},
			}
			conn, err := clickhouse.Open(opts)
			if err != nil {
				return fmt.Errorf("error connecting to clickhouse: %w", err)
			}

			var rows driver.Rows
			for {
				rows, err = conn.Query(ctx, stmt)
				if err != nil {
					if strings.Contains(err.Error(), "connection refused") {
						// The node is not ready yet, sleep and keep going
						time.Sleep(time.Millisecond * 500)
						err = nil
						continue
					}
					return fmt.Errorf("error in conn.Query for node '%s': %w", node.Name, err)
				}
				var count uint64
				for rows.Next() {
					err = rows.Scan(&count)
					if err != nil {
						return fmt.Errorf("error in rows.Scan: %w", err)
					}
				}
				if count < 1 {
					// Not ready yet
					continue
				}
				break
			}
			if err != nil {
				return err
			}
			outC <- aaaa
			// The first one to finish will be out query node
			mu.Lock()
			defer mu.Unlock()
			if savedConn == nil {
				logger.Debug().Msgf("Saving node %s", node.Name)
				savedConn = conn
			} else {
				logger.Debug().Msgf("NOT Saving node %s", node.Name)
				defer conn.Close()
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		logger := zerolog.Ctx(ctx)
		logger.Error().Err(err).Msg("error waiting for nodes")
		// return fmt.Errorf("error waiting for nodes: %w", err)
	}
	close(outC)

	for i := range outC {
		logger.Debug().Msgf("Node %s is ready", i)
		// TODO: Check if we got all the nodes ready in time
	}
	logger.Debug().Msgf("Nodes ready in %s", time.Since(s))
	s = time.Now()

	if savedConn == nil {
		return nil, errors.New("no saved conn, were you on the wireguard network?")
	}

	// Run init queries
	for idx, query := range input.InitQueries {
		s := time.Now()
		err = savedConn.Exec(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("error in init query %d: %w", idx, err)
		}
		logger.Debug().Msgf("Init query %d ran in %s", idx, time.Since(s))
	}

	s = time.Now()
	rows, err := savedConn.Query(ctx, input.Query)
	if err != nil {
		return nil, fmt.Errorf("error in Query: %w", err)
	}

	cols := rows.Columns()

	out := &WaitAndQueryOutput{Cols: cols}

	for rows.Next() {
		row := make([]any, 0)
		types := rows.ColumnTypes()
		for _, t := range types {
			row = append(row, reflect.New(t.ScanType()).Interface())
		}

		err := rows.Scan(row...)
		if err != nil {
			return nil, fmt.Errorf("error in rows.Scan: %w", err)
		}

		out.Rows = append(out.Rows, row)
	}

	logger.Debug().Msgf("Query complete in in %s", time.Since(s))

	return out, nil
}

type DeleteNodesInput struct {
	IDs []string
}

func (ac *QueryExecutorActivities) DeleteNodes(ctx context.Context, input DeleteNodesInput) error {
	for _, id := range input.IDs {
		// TODO: retries for deletion of 5xx codes
		err := fly.DeleteFlyMachine(ctx, id)
		if err != nil {
			return fmt.Errorf("error in fly.DeleteFlyMachine for id %s: %w", id, err)
		}
	}
	return nil
}

type (
	ExecuteQueryInput struct {
		Query, NodePort string
	}
	ExecuteQueryOutput struct {
		Runtime time.Duration
		Cols    []string
		Rows    []any
	}
)

func (ac *QueryExecutorActivities) ExecuteQuery(ctx context.Context, input ExecuteQueryInput) (*ExecuteQueryOutput, error) {
	opts := &clickhouse.Options{
		Addr: []string{input.NodePort},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
		},
		DialTimeout:     time.Second * 5,
		MaxOpenConns:    1,
		MaxIdleConns:    0,
		ConnMaxLifetime: 2 * time.Minute,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("error connecting to clickhouse: %w", err)
	}
	defer conn.Close()

	rows, err := conn.Query(ctx, input.Query)

	cols := rows.Columns()

	out := &ExecuteQueryOutput{Cols: cols}

	for rows.Next() {
		row := make([]any, 0)
		types := rows.ColumnTypes()
		for _, t := range types {
			row = append(row, reflect.New(t.ScanType()).Interface())
		}

		err := rows.Scan(row...)
		if err != nil {
			return nil, fmt.Errorf("error in rows.Scan: %w", err)
		}

		out.Rows = append(out.Rows, row)
	}
	return out, nil
}
