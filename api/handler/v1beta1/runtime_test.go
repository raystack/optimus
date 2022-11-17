package v1beta1_test

import (
	"context"
	"testing"

	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"

	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

func TestRuntimeServiceServer(t *testing.T) {
	log := log.NewNoop()
	ctx := context.Background()

	t.Run("Version", func(t *testing.T) {
		t.Run("should save specs and return with data", func(t *testing.T) {
			Version := "1.0.1"

			runtimeServiceServer := v1.NewRuntimeServiceServer(log, Version)
			versionRequest := pb.VersionRequest{Client: Version}
			resp, err := runtimeServiceServer.Version(ctx, &versionRequest)
			assert.Nil(t, err)
			assert.Equal(t, Version, resp.Server)
			assert.Equal(t, &pb.VersionResponse{Server: Version}, resp)
		})
	})
}
