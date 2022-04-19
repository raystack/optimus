package docker

import "github.com/docker/docker/client"

type EnvClientFactory struct {
}

func (d EnvClientFactory) New() (*client.Client, error) {
	return client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
}
