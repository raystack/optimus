package compiler_test

import (
	"testing"

	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestCompileConfigs(t *testing.T) {
	engine := compiler.NewGoEngine()
	t.Run("CompileConfigs", func(t *testing.T) {
		t.Run("compiles the configs", func(t *testing.T) {
			configs := []models.JobSpecConfigItem{
				{
					Name:  "BQ_VAL",
					Value: "22",
				},
				{
					Name:  "EXECT",
					Value: "{{.EXECUTION_TIME}}",
				},
				{
					Name:  "BUCKET",
					Value: "{{.GLOBAL__bucket}}",
				},
				{
					Name:  "BUCKETX",
					Value: "{{.proj.bucket}}",
				},
				{
					Name:  "LOAD_METHOD",
					Value: "MERGE",
				},
			}

			projContext := map[string]string{
				"bucket": "gs://project_bucket",
			}
			templateContext := map[string]interface{}{
				"EXECUTION_TIME": "2022-02-02",
				"GLOBAL__bucket": "gs://global_bucket",
				"proj":           projContext,
			}

			cmplr := compiler.NewJobConfigCompiler(engine)
			confs, err := cmplr.CompileConfigs(configs, templateContext)
			assert.Nil(t, err)

			assert.Equal(t, 5, len(confs.Configs))
			assert.Equal(t, 0, len(confs.Secrets))

			assert.Equal(t, "22", confs.Configs["BQ_VAL"])
			assert.Equal(t, "2022-02-02", confs.Configs["EXECT"])
			assert.Equal(t, "gs://global_bucket", confs.Configs["BUCKET"])
			assert.Equal(t, "gs://project_bucket", confs.Configs["BUCKETX"])
			assert.Equal(t, "MERGE", confs.Configs["LOAD_METHOD"])
		})
		t.Run("compiles the including secrets", func(t *testing.T) {
			configs := []models.JobSpecConfigItem{
				{
					Name:  "BUCKET",
					Value: "{{.GLOBAL__bucket}}",
				},
				{
					Name:  "BUCKETX",
					Value: "{{.secret.bucket}}",
				},
				{
					Name:  "LOAD_METHOD",
					Value: "MERGE",
				},
			}

			projContext := map[string]string{
				"bucket": "gs://project_bucket",
			}
			secretContext := map[string]string{
				"bucket": "gs://secret_bucket",
			}
			templateContext := map[string]interface{}{
				"GLOBAL__bucket": "gs://global_bucket",
				"proj":           projContext,
				"secret":         secretContext,
			}

			cmplr := compiler.NewJobConfigCompiler(engine)
			confs, err := cmplr.CompileConfigs(configs, templateContext)
			assert.Nil(t, err)

			assert.Equal(t, 2, len(confs.Configs))
			assert.Equal(t, "gs://global_bucket", confs.Configs["BUCKET"])
			assert.Equal(t, "MERGE", confs.Configs["LOAD_METHOD"])

			assert.Equal(t, 1, len(confs.Secrets))
			assert.Equal(t, "gs://secret_bucket", confs.Secrets["BUCKETX"])
		})
	})
}
