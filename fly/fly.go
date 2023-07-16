package fly

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/danthegoodman1/BigHouse/utils"
	"io"
	"net/http"
	"time"
)

type FlyMachine struct {
	Id         string `json:"id"`
	Name       string `json:"name"`
	State      string `json:"state"`
	Region     string `json:"region"`
	InstanceId string `json:"instance_id"`
	PrivateIp  string `json:"private_ip"`
	Config     struct {
		Env struct {
			APPENV string `json:"APP_ENV"`
		} `json:"env"`
		Init struct {
			Exec       interface{} `json:"exec"`
			Entrypoint interface{} `json:"entrypoint"`
			Cmd        interface{} `json:"cmd"`
			Tty        bool        `json:"tty"`
		} `json:"init"`
		Image    string      `json:"image"`
		Metadata interface{} `json:"metadata"`
		Restart  struct {
			Policy string `json:"policy"`
		} `json:"restart"`
		Services []struct {
			InternalPort int `json:"internal_port"`
			Ports        []struct {
				Handlers []string `json:"handlers"`
				Port     int      `json:"port"`
			} `json:"ports"`
			Protocol string `json:"protocol"`
		} `json:"services"`
		Guest struct {
			CpuKind  string `json:"cpu_kind"`
			Cpus     int    `json:"cpus"`
			MemoryMb int    `json:"memory_mb"`
		} `json:"guest"`
		Checks struct {
			Httpget struct {
				Type     string `json:"type"`
				Port     int    `json:"port"`
				Interval string `json:"interval"`
				Timeout  string `json:"timeout"`
				Method   string `json:"method"`
				Path     string `json:"path"`
			} `json:"httpget"`
		} `json:"checks"`
		ImageRef struct {
			Registry   string `json:"registry"`
			Repository string `json:"repository"`
			Tag        string `json:"tag"`
			Digest     string `json:"digest"`
			Labels     struct {
			} `json:"labels"`
		} `json:"image_ref"`
		CreatedAt time.Time `json:"created_at"`
	} `json:"config"`
}

var (
	ErrFlyHighStatusCode = errors.New("fly high status code")
)

func doFlyMachineReq(ctx context.Context, path string, body []byte) (*FlyMachine, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("https://api.machines.dev/v1/apps/%s%s", utils.FLY_APP, path), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("error in http.NewRequest: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", utils.FLY_API_TOKEN)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error in http.Do: %w", err)
	}

	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("error in io.ReadAll: %w", err)
	}

	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("high status code %d: %s :: %w", res.StatusCode, string(resBody), ErrFlyHighStatusCode)
	}

	var fm FlyMachine
	err = json.Unmarshal(resBody, &fm)
	if err != nil {
		return nil, fmt.Errorf("error in json.Unmarshal of fly response: %w", err)
	}

	return &fm, nil
}

func CreateMinimalCHMachine(ctx context.Context, name string) (*FlyMachine, error) {
	fm, err := doFlyMachineReq(ctx, "/machines", []byte(fmt.Sprintf(`
		{
		  "name": "%s",
		  "config": {
			"image": "clickhouse/clickhouse-server:23.6"
		  }
		}
	`, name)))
	if err != nil {
		return nil, fmt.Errorf("error in doFlyMachineReq: %w", err)
	}

	return fm, nil
}

func UpdateFlyCHMachine(ctx context.Context, id, name, keeperHost, keeperPort, remoteReplicas, shard, cluster, replica string) (*FlyMachine, error) {
	jBytes, err := json.Marshal(map[string]any{
		"name": name,
		"config": map[string]any{
			"image": "clickhouse/clickhouse-server:23.6",
			"size":  "performance-2x",
			"env": map[string]any{
				"ZK_HOST_1":       keeperHost,
				"ZK_PORT_1":       keeperPort,
				"REMOTE_REPLICAS": remoteReplicas,
				"SHARD":           shard,
				"CLUSTER":         cluster,
				"REPLICA":         replica,
			},
			"services": []map[string]any{
				{
					"ports": []map[string]int{
						{
							"port": 9000,
						},
					},
					"protocol":      "tcp",
					"internal_port": 9000,
				},
				{
					"ports": []map[string]int{
						{
							"port": 8123,
						},
					},
					"protocol":      "tcp",
					"internal_port": 8123,
				},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("error in json.Marshal: %w", err)
	}
	fm, err := doFlyMachineReq(ctx, "/machines/"+id, jBytes)
	if err != nil {
		return nil, fmt.Errorf("error in doFlyMachineReq: %w", err)
	}

	return fm, nil
}