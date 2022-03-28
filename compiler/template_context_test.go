package compiler_test

import (
	"testing"

	"github.com/odpf/optimus/compiler"
	"github.com/stretchr/testify/assert"
)

func TestContextBuilder(t *testing.T) {
	t.Run("Template Context", func(t *testing.T) {
		t.Run("builds the context for compilation", func(t *testing.T) {
			projectConf := map[string]string{
				"name":    "project",
				"bucket1": "bucket_project",
			}
			nsConf := map[string]string{
				"name":    "project_from_ns",
				"bucket2": "bucket_ns",
			}
			secrets := map[string]string{
				"secret1": "secret1",
				"secret2": "secret2",
			}
			instanceConfs := map[string]string{
				"instance1":      "instance1",
				"EXECUTION_DATE": "TODAY",
			}
			ctx := compiler.PrepareContext(
				compiler.From(projectConf, nsConf).WithKeyPrefix(compiler.ProjectConfigPrefix).WithName("proj"),
				compiler.From(secrets).WithName("secret"),
				compiler.From(instanceConfs).WithName("inst").AddToContext(),
			)

			assert.NotNil(t, ctx)
			assert.Equal(t, 8, len(ctx))

			// instance AddToContext
			assert.Equal(t, "instance1", ctx["instance1"])
			assert.Equal(t, "TODAY", ctx["EXECUTION_DATE"])

			// proj prefixed config
			assert.Equal(t, "project_from_ns", ctx["GLOBAL__name"])
			assert.Equal(t, "bucket_project", ctx["GLOBAL__bucket1"])
			assert.Equal(t, "bucket_ns", ctx["GLOBAL__bucket2"])

			// proj with name
			projCtx, ok := ctx["proj"].(map[string]string)
			assert.True(t, ok)
			assert.Equal(t, 3, len(projCtx))
			assert.Equal(t, "project_from_ns", projCtx["name"])
			assert.Equal(t, "bucket_project", projCtx["bucket1"])
			assert.Equal(t, "bucket_ns", projCtx["bucket2"])

			// secret with name
			secretCtx, ok := ctx["secret"].(map[string]string)
			assert.True(t, ok)
			assert.Equal(t, 2, len(secretCtx))
			assert.Equal(t, "secret1", secretCtx["secret1"])
			assert.Equal(t, "secret2", secretCtx["secret2"])

			// inst with name
			instCtx, ok := ctx["inst"].(map[string]string)
			assert.True(t, ok)
			assert.Equal(t, 2, len(instCtx))
			assert.Equal(t, "instance1", instCtx["instance1"])
			assert.Equal(t, "TODAY", instCtx["EXECUTION_DATE"])
		})
	})
}
