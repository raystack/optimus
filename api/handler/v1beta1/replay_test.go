package v1beta1_test

import (
	"context"
	"errors"
	"testing"
	"time"

	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/core/set"
	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestReplayOnServer(t *testing.T) {
	log := log.NewNoop()
	ctx := context.Background()

	t.Run("ReplayDryRun", func(t *testing.T) {
		projectName := "a-data-project"
		jobName := "a-data-job"
		timeLayout := "2006-01-02"
		projectSpec := models.ProjectSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: projectName,
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
		}
		namespaceSpec := models.NamespaceSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "dev-test-namespace-1",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
			ProjectSpec: projectSpec,
		}
		jobSpec := models.JobSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: jobName,
			Task: models.JobSpecTask{
				Config: models.JobSpecConfigs{
					{
						Name:  "do",
						Value: "this",
					},
				},
			},
			Assets: *models.JobAssets{}.New(
				[]models.JobSpecAsset{
					{
						Name:  "query.sql",
						Value: "select * from 1",
					},
				}),
		}
		t.Run("should do replay dry run successfully", func(t *testing.T) {
			startDate := time.Date(2020, 11, 25, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, 11, 28, 0, 0, 0, 0, time.UTC)
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			dagNode := tree.NewTreeNode(jobSpec)
			dagNode.Runs.Add(time.Date(2020, 11, 25, 2, 0, 0, 0, time.UTC))
			dagNode.Runs.Add(time.Date(2020, 11, 26, 2, 0, 0, 0, time.UTC))
			dagNode.Runs.Add(time.Date(2020, 11, 27, 2, 0, 0, 0, time.UTC))
			dagNode.Runs.Add(time.Date(2020, 11, 28, 2, 0, 0, 0, time.UTC))
			replayPlan := models.ReplayPlan{ExecutionTree: dagNode}

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("ReplayDryRun", ctx, replayWorkerRequest).Return(replayPlan, nil)
			defer jobService.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayDryRunRequest{
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.ReplayDryRun(context.Background(), &replayRequest)
			assert.Nil(t, err)
			assert.Equal(t, true, replayResponse.Success)
			expectedReplayResponse, err := adapter.ToReplayExecutionTreeNode(dagNode)
			assert.Nil(t, err)
			assert.Equal(t, expectedReplayResponse.JobName, replayResponse.ExecutionTree.JobName)
			assert.Equal(t, expectedReplayResponse.Dependents, replayResponse.ExecutionTree.Dependents)
			assert.Equal(t, expectedReplayResponse.Runs, replayResponse.ExecutionTree.Runs)
		})
		t.Run("should do replay dry run including only allowed namespace successfully", func(t *testing.T) {
			startDate := time.Date(2020, 11, 25, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, 11, 28, 0, 0, 0, 0, time.UTC)
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			dagNode := tree.NewTreeNode(jobSpec)
			dagNode.Runs.Add(time.Date(2020, 11, 25, 2, 0, 0, 0, time.UTC))
			dagNode.Runs.Add(time.Date(2020, 11, 26, 2, 0, 0, 0, time.UTC))
			dagNode.Runs.Add(time.Date(2020, 11, 27, 2, 0, 0, 0, time.UTC))
			dagNode.Runs.Add(time.Date(2020, 11, 28, 2, 0, 0, 0, time.UTC))
			replayPlan := models.ReplayPlan{ExecutionTree: dagNode}

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("ReplayDryRun", ctx, replayWorkerRequest).Return(replayPlan, nil)
			defer jobService.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayDryRunRequest{
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			replayResponse, err := runtimeServiceServer.ReplayDryRun(ctx, &replayRequest)
			assert.Nil(t, err)
			assert.Equal(t, true, replayResponse.Success)
			expectedReplayResponse, err := adapter.ToReplayExecutionTreeNode(dagNode)
			assert.Nil(t, err)
			assert.Equal(t, expectedReplayResponse.JobName, replayResponse.ExecutionTree.JobName)
			assert.Equal(t, expectedReplayResponse.Dependents, replayResponse.ExecutionTree.Dependents)
			assert.Equal(t, expectedReplayResponse.Runs, replayResponse.ExecutionTree.Runs)
		})
		t.Run("should failed when replay request is invalid", func(t *testing.T) {
			startDate := time.Date(2020, 11, 25, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, 11, 24, 0, 0, 0, 0, time.UTC)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			defer jobService.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
				nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayDryRunRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceSpec.Name,
				JobName:       jobName,
				StartDate:     startDate.Format(timeLayout),
				EndDate:       endDate.Format(timeLayout),
			}
			replayResponse, err := runtimeServiceServer.ReplayDryRun(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Nil(t, replayResponse)
		})
		t.Run("should failed when unable to prepare the job specs", func(t *testing.T) {
			startDate := time.Date(2020, 11, 25, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, 11, 28, 0, 0, 0, 0, time.UTC)
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("ReplayDryRun", ctx, replayWorkerRequest).Return(models.ReplayPlan{}, errors.New("populating jobs spec failed"))
			defer jobService.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
				nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayDryRunRequest{
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.ReplayDryRun(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Nil(t, replayResponse)
		})
	})

	t.Run("Replay", func(t *testing.T) {
		projectName := "a-data-project"
		jobName := "a-data-job"
		timeLayout := "2006-01-02"
		startDate := time.Date(2020, 11, 25, 0, 0, 0, 0, time.UTC)
		endDate := time.Date(2020, 11, 28, 0, 0, 0, 0, time.UTC)
		projectSpec := models.ProjectSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: projectName,
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
		}
		namespaceSpec := models.NamespaceSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "dev-test-namespace-1",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
			ProjectSpec: projectSpec,
		}
		jobSpec := models.JobSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: jobName,
			Task: models.JobSpecTask{
				Config: models.JobSpecConfigs{
					{
						Name:  "do",
						Value: "this",
					},
				},
			},
			Assets: *models.JobAssets{}.New(
				[]models.JobSpecAsset{
					{
						Name:  "query.sql",
						Value: "select * from 1",
					},
				}),
		}
		t.Run("should do replay successfully", func(t *testing.T) {
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			randomUUID := uuid.Must(uuid.NewRandom())

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("Replay", ctx, replayWorkerRequest).Return(models.ReplayResult{ID: randomUUID}, nil)
			defer jobService.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
				nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayRequest{
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.Nil(t, err)
			assert.Equal(t, randomUUID.String(), replayResponse.Id)
		})
		t.Run("should do replay including only allowed namespace successfully", func(t *testing.T) {
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			randomUUID := uuid.Must(uuid.NewRandom())

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("Replay", ctx, replayWorkerRequest).Return(models.ReplayResult{ID: randomUUID}, nil)
			defer jobService.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
				nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayRequest{
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.Nil(t, err)
			assert.Equal(t, randomUUID.String(), replayResponse.Id)
		})
		t.Run("should failed when replay request is invalid", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).
				Return(models.NamespaceSpec{}, errors.New("Namespace not found"))
			defer namespaceService.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil,
				nil,
				nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayRequest{
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Nil(t, replayResponse)
		})
		t.Run("should failed when replay process is failed", func(t *testing.T) {
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			errMessage := "internal error"

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("Replay", ctx, replayWorkerRequest).Return(models.ReplayResult{}, errors.New(errMessage))
			defer jobService.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
				nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayRequest{
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
			assert.Equal(t, codes.Internal, status.Code(err))
			assert.Nil(t, replayResponse)
		})
		t.Run("should failed when project is not found", func(t *testing.T) {
			errMessage := "project not found"
			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).
				Return(models.NamespaceSpec{}, errors.New(errMessage))
			defer namespaceService.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil,
				nil,
				nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayRequest{
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
			assert.Nil(t, replayResponse)
		})
		t.Run("should failed when job is not found in the namespace", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			errMessage := "job not found in namespace"
			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(models.JobSpec{}, errors.New(errMessage))
			defer jobService.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
				nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayRequest{
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
			assert.Nil(t, replayResponse)
		})
		t.Run("should failed when replay validation is failed", func(t *testing.T) {
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("Replay", ctx, replayWorkerRequest).Return(models.ReplayResult{}, job.ErrConflictedJobRun)
			defer jobService.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
				nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayRequest{
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), job.ErrConflictedJobRun.Error())
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			assert.Nil(t, replayResponse)
		})
		t.Run("should failed when request queue is full", func(t *testing.T) {
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("Replay", ctx, replayWorkerRequest).Return(models.ReplayResult{}, job.ErrRequestQueueFull)
			defer jobService.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
				nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayRequest{
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), job.ErrRequestQueueFull.Error())
			assert.Equal(t, codes.Unavailable, status.Code(err))
			assert.Nil(t, replayResponse)
		})
	})

	t.Run("GetReplayStatus", func(t *testing.T) {
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: projectName,
		}
		reqUUID := uuid.Must(uuid.NewRandom())
		replayRequest := models.ReplayRequest{
			ID:      reqUUID,
			Project: projectSpec,
		}

		t.Run("should get status of each jobs and runs of a replay", func(t *testing.T) {
			jobName := "a-data-job"
			jobSpec := models.JobSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: jobName,
				Task: models.JobSpecTask{
					Config: models.JobSpecConfigs{
						{
							Name:  "do",
							Value: "this",
						},
					},
				},
				Assets: *models.JobAssets{}.New(
					[]models.JobSpecAsset{
						{
							Name:  "query.sql",
							Value: "select * from 1",
						},
					}),
			}

			jobStatusList := []models.JobStatus{
				{
					ScheduledAt: time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC),
					State:       models.RunStateRunning,
				},
				{
					ScheduledAt: time.Date(2020, 11, 12, 0, 0, 0, 0, time.UTC),
					State:       models.RunStateRunning,
				},
			}

			dagNode := tree.NewTreeNode(jobSpec)
			dagNode.Runs = set.NewTreeSetWith(job.TimeOfJobStatusComparator)
			dagNode.Runs.Add(jobStatusList[0])
			dagNode.Runs.Add(jobStatusList[1])
			replayState := models.ReplayState{
				Status: models.ReplayStatusReplayed,
				Node:   dagNode,
			}

			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetReplayStatus", context.TODO(), replayRequest).Return(replayState, nil)

			adapter := v1.NewAdapter(nil, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				nil,
				projectService,
				nil,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			expectedReplayStatusNodeResponse, err := adapter.ToReplayStatusTreeNode(replayState.Node)
			assert.Nil(t, err)

			replayRequestPb := pb.GetReplayStatusRequest{
				Id:          reqUUID.String(),
				ProjectName: projectName,
			}
			replayStatusResponse, err := runtimeServiceServer.GetReplayStatus(ctx, &replayRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, models.ReplayStatusReplayed, replayStatusResponse.State)
			assert.Equal(t, expectedReplayStatusNodeResponse.Runs, replayStatusResponse.Response.Runs)
		})
		t.Run("should failed when unable to get status of a replay", func(t *testing.T) {
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			errMessage := "internal error"
			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetReplayStatus", context.TODO(), replayRequest).Return(models.ReplayState{}, errors.New(errMessage))

			adapter := v1.NewAdapter(nil, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				nil,
				projectService,
				nil,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			replayRequestPb := pb.GetReplayStatusRequest{
				Id:          reqUUID.String(),
				ProjectName: projectName,
			}
			replayStatusResponse, err := runtimeServiceServer.GetReplayStatus(ctx, &replayRequestPb)

			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
			assert.Nil(t, replayStatusResponse)
		})
	})

	t.Run("ListReplays", func(t *testing.T) {
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: projectName,
		}

		t.Run("should get list of replay for a project", func(t *testing.T) {
			jobName := "a-data-job"
			jobSpec := models.JobSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: jobName,
				Task: models.JobSpecTask{
					Config: models.JobSpecConfigs{
						{
							Name:  "do",
							Value: "this",
						},
					},
				},
				Assets: *models.JobAssets{}.New(
					[]models.JobSpecAsset{
						{
							Name:  "query.sql",
							Value: "select * from 1",
						},
					}),
			}

			replaySpecs := []models.ReplaySpec{
				{
					ID:        uuid.Must(uuid.NewRandom()),
					Job:       jobSpec,
					StartDate: time.Date(2020, 11, 25, 0, 0, 0, 0, time.UTC),
					EndDate:   time.Date(2020, 11, 28, 0, 0, 0, 0, time.UTC),
					Status:    models.ReplayStatusReplayed,
					CreatedAt: time.Date(2021, 8, 1, 0, 0, 0, 0, time.UTC),
				},
				{
					ID:        uuid.Must(uuid.NewRandom()),
					Job:       jobSpec,
					StartDate: time.Date(2020, 12, 25, 0, 0, 0, 0, time.UTC),
					EndDate:   time.Date(2020, 12, 28, 0, 0, 0, 0, time.UTC),
					Status:    models.ReplayStatusInProgress,
					CreatedAt: time.Date(2021, 8, 2, 0, 0, 0, 0, time.UTC),
				},
			}
			expectedReplayList := &pb.ListReplaysResponse{
				ReplayList: []*pb.ReplaySpec{
					{
						Id:        replaySpecs[0].ID.String(),
						JobName:   jobSpec.Name,
						StartDate: timestamppb.New(time.Date(2020, 11, 25, 0, 0, 0, 0, time.UTC)),
						EndDate:   timestamppb.New(time.Date(2020, 11, 28, 0, 0, 0, 0, time.UTC)),
						State:     models.ReplayStatusReplayed,
						CreatedAt: timestamppb.New(time.Date(2021, 8, 1, 0, 0, 0, 0, time.UTC)),
					},
					{
						Id:        replaySpecs[1].ID.String(),
						JobName:   jobSpec.Name,
						StartDate: timestamppb.New(time.Date(2020, 12, 25, 0, 0, 0, 0, time.UTC)),
						EndDate:   timestamppb.New(time.Date(2020, 12, 28, 0, 0, 0, 0, time.UTC)),
						State:     models.ReplayStatusInProgress,
						CreatedAt: timestamppb.New(time.Date(2021, 8, 2, 0, 0, 0, 0, time.UTC)),
					},
				},
			}

			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetReplayList", ctx, projectSpec.ID).Return(replaySpecs, nil)

			adapter := v1.NewAdapter(nil, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				nil,
				projectService,
				nil,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			replayRequestPb := pb.ListReplaysRequest{
				ProjectName: projectName,
			}
			replayStatusResponse, err := runtimeServiceServer.ListReplays(ctx, &replayRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, expectedReplayList, replayStatusResponse)
		})
		t.Run("should failed when unable to get status of a replay", func(t *testing.T) {
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			errMessage := "internal error"
			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetReplayList", ctx, projectSpec.ID).Return([]models.ReplaySpec{}, errors.New(errMessage))

			adapter := v1.NewAdapter(nil, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				nil,
				projectService,
				nil,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			replayRequestPb := pb.ListReplaysRequest{
				ProjectName: projectName,
			}
			replayListResponse, err := runtimeServiceServer.ListReplays(ctx, &replayRequestPb)

			assert.Contains(t, err.Error(), errMessage)
			assert.Nil(t, replayListResponse)
		})
	})
}
