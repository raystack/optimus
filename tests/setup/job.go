package setup

import (
	"fmt"
	"time"

	"github.com/odpf/optimus/models"
)

func Job(i int, namespace models.NamespaceSpec, bq2bq models.DependencyResolverMod, hookUnit models.BasePlugin) models.JobSpec { //nolint:unparam
	jobConfig := []models.JobSpecConfigItem{
		{Name: "DATASET", Value: "playground"},
		{Name: "JOB_LABELS", Value: "owner=optimus"},
		{Name: "LOAD_METHOD", Value: "REPLACE"},
		{Name: "PROJECT", Value: "integration"},
		{Name: "SQL_TYPE", Value: "STANDARD"},
		{Name: "TABLE", Value: fmt.Sprintf("table%d", i)},
		{Name: "TASK_TIMEZONE", Value: "UTC"},
		{Name: "SECRET_NAME", Value: "{{.secret.secret3}}"},
		{Name: "TASK_BQ2BQ", Value: "{{.secret.TASK_BQ2BQ}}"},
	}

	jobMeta := models.JobSpecMetadata{
		Resource: models.JobSpecResource{
			Request: models.JobSpecResourceConfig{CPU: "200m", Memory: "1g"},
			Limit:   models.JobSpecResourceConfig{CPU: "1000m", Memory: "2g"},
		},
	}

	window := &&models.WindowV1{
		SizeAsDuration:   time.Hour * 24,
		OffsetAsDuration: time.Second * 0,
		TruncateTo:       "h",
	}
	var hooks []models.JobSpecHook
	if hookUnit != nil {
		hooks = append(hooks, models.JobSpecHook{
			Config: []models.JobSpecConfigItem{
				{
					Name:  "FILTER_EXPRESSION",
					Value: "event_timestamp > 10000",
				},
			},
			Unit: &models.Plugin{Base: hookUnit},
		})
	}

	jobSpec := models.JobSpec{
		Version:     1,
		Name:        fmt.Sprintf("job_%d", i),
		Description: "A test job for benchmarking deploy",
		Labels:      map[string]string{"orchestrator": "optimus"},
		Owner:       "Benchmark",
		Schedule: models.JobSpecSchedule{
			StartDate: time.Date(2022, 2, 26, 0, 0, 0, 0, time.UTC),
			EndDate:   nil,
			Interval:  "0 8 * * *",
		},
		Behavior: models.JobSpecBehavior{
			DependsOnPast: false,
			CatchUp:       false,
			Retry: models.JobSpecBehaviorRetry{
				Count:              2,
				Delay:              time.Millisecond * 100,
				ExponentialBackoff: true,
			},
			Notify: nil,
		},
		Task: models.JobSpecTask{
			Unit: &models.Plugin{
				Base:          bq2bq,
				DependencyMod: bq2bq,
			},
			Priority: 2000,
			Window:   window,
			Config:   jobConfig,
		},
		Dependencies: nil,
		Assets: *models.JobAssets{}.New(
			[]models.JobSpecAsset{
				{
					Name: "query.sql",
					Value: `WITH Characters AS
 (SELECT '{{.secret.secret3}}' as name, 51 as age, CAST("{{.DSTART}}" AS TIMESTAMP) as event_timestamp, CAST("{{.EXECUTION_TIME}}" AS TIMESTAMP) as load_timestamp UNION ALL
  SELECT 'Uchiha', 77, CAST("{{.DSTART}}" AS TIMESTAMP) as event_timestamp, CAST("{{.EXECUTION_TIME}}" AS TIMESTAMP) as load_timestamp UNION ALL
  SELECT 'Saitama', 77, CAST("{{.DSTART}}" AS TIMESTAMP) as event_timestamp, CAST("{{.EXECUTION_TIME}}" AS TIMESTAMP) as load_timestamp UNION ALL
  SELECT 'Sanchez', 52, CAST("{{.DSTART}}" AS TIMESTAMP) as event_timestamp, CAST("{{.EXECUTION_TIME}}" AS TIMESTAMP) as load_timestamp)
SELECT * FROM Characters`,
				},
			},
		),
		Hooks:    hooks,
		Metadata: jobMeta,
		ExternalDependencies: models.ExternalDependency{
			HTTPDependencies: []models.HTTPDependency{
				{
					Name: "test_http_sensor_1",
					RequestParams: map[string]string{
						"key_test": "value_test",
					},
					URL: "http://test/optimus/status/1",
					Headers: map[string]string{
						"Content-Type": "application/json",
					},
				},
			},
		},
		NamespaceSpec: namespace,
	}

	return jobSpec
}
