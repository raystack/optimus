package model_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/odpf/optimus/client/local/model"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type JobSpecTestSuite struct {
	suite.Suite
}

func TestJobSpecTestSuite(t *testing.T) {
	s := new(JobSpecTestSuite)
	suite.Run(t, s)
}

func (s *JobSpecTestSuite) TestToProto() {
	s.Run("should return job spec proto with behavior proto nil when behavior.retry is nil and behavior.notify is empty", func() {
		jobSpec := createCompleteJobSpec()
		jobSpec.Behavior.Retry = nil
		jobSpec.Behavior.Notify = []model.JobSpecBehaviorNotifier{}

		expectedProto := createCompleteJobSpecProto()
		expectedProto.Behavior = nil
		jobSpecProto := jobSpec.ToProto()
		s.Assert().EqualValues(expectedProto, jobSpecProto)
	})

	s.Run("should return job spec proto with metadata proto nil when job spec metadata is nil", func() {
		jobSpec := createCompleteJobSpec()
		jobSpec.Metadata = nil

		expectedProto := createCompleteJobSpecProto()
		expectedProto.Metadata = nil
		jobSpecProto := jobSpec.ToProto()
		s.Assert().EqualValues(expectedProto, jobSpecProto)
	})

	s.Run("should return job spec proto with metadata resource config proto nil when metadata.resource config is nil", func() {
		jobSpec := createCompleteJobSpec()
		jobSpec.Metadata.Resource.Request = nil

		expectedProto := createCompleteJobSpecProto()
		expectedProto.Metadata.Resource.Request = nil
		jobSpecProto := jobSpec.ToProto()
		s.Assert().EqualValues(expectedProto, jobSpecProto)
	})

	s.Run("should return complete job spec proto when job spec is complete", func() {
		jobSpec := createCompleteJobSpec()
		expectedProto := createCompleteJobSpecProto()

		jobSpecProto := jobSpec.ToProto()
		s.Assert().EqualValues(expectedProto, jobSpecProto)
	})
}

func createCompleteJobSpec() model.JobSpec {
	return model.JobSpec{
		Version:     1,
		Name:        "job_1",
		Owner:       "optimus@optimus.dev",
		Description: "job one",
		Schedule: model.JobSpecSchedule{
			StartDate: "30-09-2022",
			EndDate:   "01-01-2050",
			Interval:  "12 10 * * *",
		},
		Behavior: model.JobSpecBehavior{
			DependsOnPast: true,
			Catchup:       true,
			Retry: &model.JobSpecBehaviorRetry{
				Count:              10,
				Delay:              2 * time.Second,
				ExponentialBackoff: true,
			},
			Notify: []model.JobSpecBehaviorNotifier{
				{
					On: "failure",
					Config: map[string]string{
						"configkey": "configvalue",
					},
					Channels: []string{"slack://#optimus"},
				},
			},
		},
		Task: model.JobSpecTask{
			Name: "job_task_1",
			Config: map[string]string{
				"taskkey": "taskvalue",
			},
			Window: model.JobSpecTaskWindow{
				Size:       "24h",
				Offset:     "1h",
				TruncateTo: "d",
			},
		},
		Asset: map[string]string{
			"query.sql": "SELECT * FROM example",
		},
		Labels: map[string]string{
			"orchestrator": "optimus",
		},
		Dependencies: []model.JobSpecDependency{
			{
				JobName: "job_name_1",
				Type:    "extra",
				HTTP: &model.JobSpecDependencyHTTP{
					Name: "http_dep",
					RequestParams: map[string]string{
						"param1": "paramvalue",
					},
					URL: "http://optimus.dev/example",
					Headers: map[string]string{
						"User-Agent": "*",
					},
				},
			},
			{
				JobName: "job_name_2",
				Type:    "intra",
			},
		},
		Hooks: []model.JobSpecHook{
			{
				Name: "hook_1",
				Config: map[string]string{
					"hookkey": "hookvalue",
				},
			},
		},
		Metadata: &model.JobSpecMetadata{
			Resource: &model.JobSpecMetadataResource{
				Request: &model.JobSpecMetadataResourceConfig{
					CPU:    "250m",
					Memory: "64Mi",
				},
				Limit: &model.JobSpecMetadataResourceConfig{
					CPU:    "500m",
					Memory: "128Mi",
				},
			},
			Airflow: &model.JobSpecMetadataAirflow{
				Pool:  "poolA",
				Queue: "queueA",
			},
		},
	}
}

func createCompleteJobSpecProto() *pb.JobSpecification {
	return &pb.JobSpecification{
		Version:       1,
		Name:          "job_1",
		Owner:         "optimus@optimus.dev",
		StartDate:     "30-09-2022",
		EndDate:       "01-01-2050",
		Interval:      "12 10 * * *",
		DependsOnPast: true,
		CatchUp:       true,
		Behavior: &pb.JobSpecification_Behavior{
			Retry: &pb.JobSpecification_Behavior_Retry{
				Count:              10,
				Delay:              durationpb.New(2 * time.Second),
				ExponentialBackoff: true,
			},
			Notify: []*pb.JobSpecification_Behavior_Notifiers{
				{
					On: pb.JobEvent_TYPE_FAILURE,
					Config: map[string]string{
						"configkey": "configvalue",
					},
					Channels: []string{"slack://#optimus"},
				},
			},
		},
		TaskName: "job_task_1",
		Config: []*pb.JobConfigItem{
			{
				Name:  "taskkey",
				Value: "taskvalue",
			},
		},
		WindowSize:       "24h",
		WindowOffset:     "1h",
		WindowTruncateTo: "d",
		Assets: map[string]string{
			"query.sql": "SELECT * FROM example",
		},
		Labels: map[string]string{
			"orchestrator": "optimus",
		},
		Dependencies: []*pb.JobDependency{
			{
				Name: "job_name_1",
				Type: "extra",
				HttpDependency: &pb.HttpDependency{
					Name: "http_dep",
					Params: map[string]string{
						"param1": "paramvalue",
					},
					Url: "http://optimus.dev/example",
					Headers: map[string]string{
						"User-Agent": "*",
					},
				},
			},
			{
				Name: "job_name_2",
				Type: "intra",
			},
		},
		Metadata: &pb.JobMetadata{
			Resource: &pb.JobSpecMetadataResource{
				Request: &pb.JobSpecMetadataResourceConfig{
					Cpu:    "250m",
					Memory: "64Mi",
				},
				Limit: &pb.JobSpecMetadataResourceConfig{
					Cpu:    "500m",
					Memory: "128Mi",
				},
			},
			Airflow: &pb.JobSpecMetadataAirflow{
				Pool:  "poolA",
				Queue: "queueA",
			},
		},
	}
}
