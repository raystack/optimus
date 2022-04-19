package docker

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/docker/docker/api/types/filters"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/odpf/optimus/models"
)

const (
	OptimusInstanceIDLabel = "OPTIMUS_INSTANCE_ID"
)

type ClientFactory interface {
	New() (*client.Client, error)
}

// Executor
// Read more about docker APIs at https://docs.docker.com/engine/api/v1.41
type Executor struct {
	mu            *sync.Mutex
	hostname      string
	now           func() time.Time
	clientFactory ClientFactory
}

func NewExecutor(factory ClientFactory, hostname string) *Executor {
	return &Executor{
		mu:            new(sync.Mutex),
		hostname:      hostname,
		now:           time.Now().UTC,
		clientFactory: factory,
	}
}

// Start container
func (e *Executor) Start(ctx context.Context, req *models.ExecutorStartRequest) (*models.ExecutorStartResponse, error) {
	cli, err := e.clientFactory.New()
	if err != nil {
		return nil, err
	}
	if _, err = cli.ImagePull(ctx, req.Unit.Info().Image, types.ImagePullOptions{}); err != nil {
		return nil, err
	}

	// build envs
	var envs []string
	envs = append(envs, fmt.Sprintf("JOB_NAME=%s", req.JobName))
	envs = append(envs, fmt.Sprintf("PROJECT=%s", req.Namespace.ProjectSpec.Name))
	envs = append(envs, fmt.Sprintf("NAMESPACE=%s", req.Namespace.Name))
	envs = append(envs, fmt.Sprintf("INSTANCE_TYPE=%s", req.Type.String()))
	envs = append(envs, fmt.Sprintf("INSTANCE_NAME=%s", req.Unit.Info().Name))
	envs = append(envs, fmt.Sprintf("OPTIMUS_HOSTNAME=%s", e.hostname))
	envs = append(envs, fmt.Sprintf("SCHEDULED_AT=%s", e.now().Format(models.InstanceScheduledAtTimeLayout)))
	envs = append(envs, fmt.Sprintf("JOB_DIR=/data"))
	req.JobLabels[OptimusInstanceIDLabel] = req.InstanceID

	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Env:    envs,
		Image:  req.Unit.Info().Image,
		Labels: req.JobLabels,
	}, nil, nil, nil, "")
	if err != nil {
		return nil, err
	}

	if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		return nil, err
	}
	return &models.ExecutorStartResponse{}, nil
}

func (e *Executor) Stop(ctx context.Context, req *models.ExecutorStopRequest) error {
	cli, err := e.clientFactory.New()
	if err != nil {
		return err
	}
	instanceContainer, err := e.fetchContainer(ctx, req.ID)
	if err != nil {
		return err
	}
	return cli.ContainerStop(ctx, instanceContainer.ID, nil)
}

func (e *Executor) WaitForFinish(ctx context.Context, instanceID string) (int, error) {
	cli, err := e.clientFactory.New()
	if err != nil {
		return 1, err
	}
	instanceContainer, err := e.fetchContainer(ctx, instanceID)
	if err != nil {
		return 1, err
	}

	statusCh, errCh := cli.ContainerWait(ctx, instanceContainer.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return 1, err
		}
	case status := <-statusCh:
		return int(status.StatusCode), nil
	}
	return 0, nil
}

func (e *Executor) Stats(ctx context.Context, instanceID string) (*models.ExecutorStats, error) {
	cli, err := e.clientFactory.New()
	if err != nil {
		return nil, err
	}
	instanceContainer, err := e.fetchContainer(ctx, instanceID)
	if err != nil {
		return nil, err
	}

	// prepare container status
	containerStats, err := cli.ContainerInspect(ctx, instanceContainer.ID)
	if err != nil {
		return nil, err
	}
	currentStatus := models.ExecutorStatusRunning
	switch containerStats.State.Status {
	case "created", "running", "paused", "restarting":
		break
	case "dead", "exited":
		if containerStats.State.ExitCode == 0 {
			currentStatus = models.ExecutorStatusSuccess
		} else {
			currentStatus = models.ExecutorStatusFailed
		}
	}

	// fetch logs
	containerLogs, err := cli.ContainerLogs(ctx, instanceContainer.ID, types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return nil, err
	}
	logBytes, err := io.ReadAll(containerLogs)
	if err != nil {
		return nil, err
	}
	_ = containerLogs.Close()

	return &models.ExecutorStats{
		Logs:     logBytes,
		Status:   currentStatus,
		ExitCode: containerStats.State.ExitCode,
	}, nil
}

func (e *Executor) fetchContainer(ctx context.Context, instanceID string) (types.Container, error) {
	cli, err := e.clientFactory.New()
	if err != nil {
		return types.Container{}, err
	}

	containers, err := cli.ContainerList(ctx, types.ContainerListOptions{
		All:   true,
		Limit: 1,
		Filters: filters.NewArgs(
			filters.Arg(OptimusInstanceIDLabel, instanceID),
		),
	})
	if err != nil {
		return types.Container{}, err
	}
	if len(containers) == 0 {
		return types.Container{}, fmt.Errorf("not found")
	}
	return containers[0], nil
}
