package v1beta1_test

import (
	"context"
	"testing"

	"github.com/raystack/salt/log"
	"github.com/stretchr/testify/assert"

	pb "github.com/raystack/optimus/protos/raystack/optimus/core/v1beta1"
	v1 "github.com/raystack/optimus/server/handler/v1beta1"
)

func TestVersionHandler(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()

	t.Run("Version", func(t *testing.T) {
		t.Run("returns the version of server", func(t *testing.T) {
			version := "1.0.1"

			versionHandler := v1.NewVersionHandler(logger, version)
			versionRequest := pb.VersionRequest{Client: version}

			resp, err := versionHandler.Version(ctx, &versionRequest)
			assert.NoError(t, err)
			assert.Equal(t, version, resp.Server)
		})
	})
}
