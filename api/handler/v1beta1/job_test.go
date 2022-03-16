package v1beta1_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/core/progress"

	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type RuntimeServiceServerTestSuite struct {
	suite.Suite
	version          string
	ctx              context.Context //nolint:containedctx
	namespaceService *mock.NamespaceService
	projectService   *mock.ProjectService
	secretService    *mock.SecretService
	runService       *mock.RunService        // TODO: refactor to service package
	jobService       *mock.JobService        // TODO: refactor to service package
	resourceService  models.DatastoreService // TODO: refactor to service package
	jobEventService  v1.JobEventService      // TODO: refactor to service package
	adapter          *mock.ProtoAdapter
	scheduler        models.SchedulerUnit
	log              log.Logger
	progressObserver progress.Observer

	req           *pb.DeployJobSpecificationRequest
	projectSpec   models.ProjectSpec
	namespaceSpec models.NamespaceSpec
}

func (s *RuntimeServiceServerTestSuite) SetupTest() {
	s.version = "v1.0.0"
	s.ctx = context.Background()
	s.namespaceService = new(mock.NamespaceService)
	s.adapter = new(mock.ProtoAdapter)
	s.jobService = new(mock.JobService)
	s.log = log.NewNoop()
	// ... etdc

	s.projectSpec = models.ProjectSpec{}
	s.projectSpec.Name = "project-a"
	s.projectSpec.ID = uuid.MustParse("26a0d6a0-13c6-4b30-ae6f-29233df70f31")

	s.namespaceSpec = models.NamespaceSpec{}
	s.namespaceSpec.Name = "ns1"
	s.namespaceSpec.ID = uuid.MustParse("ceba7919-e07d-48b4-a4ce-141d79a3b59d")

	s.req = &pb.DeployJobSpecificationRequest{}
	s.req.ProjectName = s.projectSpec.Name
	s.req.NamespaceName = s.namespaceSpec.Name
}

func TestRuntimeServiceServerTestSuite(t *testing.T) {
	s := new(RuntimeServiceServerTestSuite)
	suite.Run(t, s)
}

func (s *RuntimeServiceServerTestSuite) newRuntimeServiceServer() *v1.RuntimeServiceServer {
	return v1.NewRuntimeServiceServer(
		s.log,
		s.version,
		s.jobService,
		s.jobEventService,
		s.resourceService,
		s.projectService,
		s.namespaceService,
		s.secretService,
		s.adapter,
		s.progressObserver,
		s.runService,
		s.scheduler,
	)
}

func (s *RuntimeServiceServerTestSuite) TestDeployJobSpecification_Success() {
	s.Run("NoJobSpec", func() {
		s.SetupTest()
		stream := new(mock.DeployJobSpecificationServer)
		stream.On("Context").Return(s.ctx)
		stream.On("Recv").Return(s.req, nil).Once()
		stream.On("Recv").Return(nil, io.EOF).Once()

		s.namespaceService.On("Get", s.ctx, s.req.GetProjectName(), s.req.GetNamespaceName()).Return(s.namespaceSpec, nil).Once()
		s.jobService.On("KeepOnly", s.ctx, s.namespaceSpec, mock2.Anything, mock2.Anything).Return(nil).Once()
		s.jobService.On("Sync", s.ctx, s.namespaceSpec, mock2.Anything).Return(nil).Once()
		stream.On("Send", mock2.Anything).Return(nil).Once()

		runtimeServiceServer := s.newRuntimeServiceServer()
		err := runtimeServiceServer.DeployJobSpecification(stream)

		s.Assert().NoError(err)
		stream.AssertExpectations(s.T())
		s.namespaceService.AssertExpectations(s.T())
		s.jobService.AssertExpectations(s.T())
	})

	s.Run("TwoJobSpec", func() {
		s.SetupTest()
		jobSpecs := []*pb.JobSpecification{}
		jobSpecs = append(jobSpecs, &pb.JobSpecification{Name: "job-1"})
		jobSpecs = append(jobSpecs, &pb.JobSpecification{Name: "job-2"})
		s.req.Jobs = jobSpecs
		adaptedJobs := []models.JobSpec{}
		adaptedJobs = append(adaptedJobs, models.JobSpec{Name: "job-1"})
		adaptedJobs = append(adaptedJobs, models.JobSpec{Name: "job-2"})

		stream := new(mock.DeployJobSpecificationServer)
		stream.On("Context").Return(s.ctx)
		stream.On("Recv").Return(s.req, nil).Once()
		stream.On("Recv").Return(nil, io.EOF).Once()

		s.namespaceService.On("Get", s.ctx, s.req.GetProjectName(), s.req.GetNamespaceName()).Return(s.namespaceSpec, nil).Once()
		for i := range jobSpecs {
			s.adapter.On("FromJobProto", jobSpecs[i]).Return(adaptedJobs[i], nil).Once()
			s.jobService.On("Create", s.ctx, s.namespaceSpec, adaptedJobs[i]).Return(nil).Once()
		}
		s.jobService.On("KeepOnly", s.ctx, s.namespaceSpec, adaptedJobs, mock2.Anything).Return(nil).Once()
		s.jobService.On("Sync", s.ctx, s.namespaceSpec, mock2.Anything).Return(nil).Once()
		stream.On("Send", mock2.Anything).Return(nil).Once()

		runtimeServiceServer := s.newRuntimeServiceServer()
		err := runtimeServiceServer.DeployJobSpecification(stream)

		s.Assert().NoError(err)
		stream.AssertExpectations(s.T())
		s.adapter.AssertExpectations(s.T())
		s.namespaceService.AssertExpectations(s.T())
		s.jobService.AssertExpectations(s.T())
	})
}

func (s *RuntimeServiceServerTestSuite) TestDeployJobSpecification_Fail() {
	s.Run("StreamRecvError", func() {
		s.SetupTest()
		stream := new(mock.DeployJobSpecificationServer)
		stream.On("Recv").Return(nil, errors.New("any error")).Once()
		stream.On("Send", mock2.Anything).Return(nil).Once()

		runtimeServiceServer := s.newRuntimeServiceServer()
		err := runtimeServiceServer.DeployJobSpecification(stream)

		s.Assert().Error(err)
		stream.AssertExpectations(s.T())
	})

	s.Run("NamespaceServiceGetError", func() {
		s.SetupTest()
		stream := new(mock.DeployJobSpecificationServer)
		stream.On("Context").Return(s.ctx)
		stream.On("Recv").Return(s.req, nil).Once()
		stream.On("Recv").Return(nil, io.EOF).Once()

		s.namespaceService.On("Get", s.ctx, s.req.GetProjectName(), s.req.GetNamespaceName()).Return(models.NamespaceSpec{}, errors.New("any error")).Once()
		stream.On("Send", mock2.Anything).Return(nil).Once()

		runtimeServiceServer := s.newRuntimeServiceServer()
		err := runtimeServiceServer.DeployJobSpecification(stream)

		s.Assert().Error(err)
		stream.AssertExpectations(s.T())
		s.namespaceService.AssertExpectations(s.T())
	})

	s.Run("AdapterFromJobProtoError", func() {
		s.SetupTest()
		jobSpecs := []*pb.JobSpecification{}
		jobSpecs = append(jobSpecs, &pb.JobSpecification{Name: "job-1"})
		s.req.Jobs = jobSpecs

		stream := new(mock.DeployJobSpecificationServer)
		stream.On("Context").Return(s.ctx)
		stream.On("Recv").Return(s.req, nil).Once()
		stream.On("Recv").Return(nil, io.EOF).Once()

		s.namespaceService.On("Get", s.ctx, s.req.GetProjectName(), s.req.GetNamespaceName()).Return(s.namespaceSpec, nil).Once()
		s.adapter.On("FromJobProto", jobSpecs[0]).Return(models.JobSpec{}, errors.New("any error")).Once()
		stream.On("Send", mock2.Anything).Return(nil).Once()

		runtimeServiceServer := s.newRuntimeServiceServer()
		err := runtimeServiceServer.DeployJobSpecification(stream)

		s.Assert().Error(err)
		stream.AssertExpectations(s.T())
		s.adapter.AssertExpectations(s.T())
		s.namespaceService.AssertExpectations(s.T())
	})

	s.Run("JobServiceCreateError", func() {
		s.SetupTest()
		jobSpecs := []*pb.JobSpecification{}
		jobSpecs = append(jobSpecs, &pb.JobSpecification{Name: "job-1"})
		s.req.Jobs = jobSpecs
		adaptedJobs := []models.JobSpec{}
		adaptedJobs = append(adaptedJobs, models.JobSpec{Name: "job-1"})

		stream := new(mock.DeployJobSpecificationServer)
		stream.On("Context").Return(s.ctx)
		stream.On("Recv").Return(s.req, nil).Once()
		stream.On("Recv").Return(nil, io.EOF).Once()

		s.namespaceService.On("Get", s.ctx, s.req.GetProjectName(), s.req.GetNamespaceName()).Return(s.namespaceSpec, nil).Once()
		s.adapter.On("FromJobProto", jobSpecs[0]).Return(adaptedJobs[0], nil).Once()
		s.jobService.On("Create", s.ctx, s.namespaceSpec, adaptedJobs[0]).Return(errors.New("any error")).Once()
		stream.On("Send", mock2.Anything).Return(nil).Once()

		runtimeServiceServer := s.newRuntimeServiceServer()
		err := runtimeServiceServer.DeployJobSpecification(stream)

		s.Assert().Error(err)
		stream.AssertExpectations(s.T())
		s.adapter.AssertExpectations(s.T())
		s.namespaceService.AssertExpectations(s.T())
		s.jobService.AssertExpectations(s.T())
	})

	s.Run("JobServiceKeepOnlyError", func() {
		s.SetupTest()
		stream := new(mock.DeployJobSpecificationServer)
		stream.On("Context").Return(s.ctx)
		stream.On("Recv").Return(s.req, nil).Once()
		stream.On("Recv").Return(nil, io.EOF).Once()

		s.namespaceService.On("Get", s.ctx, s.req.GetProjectName(), s.req.GetNamespaceName()).Return(s.namespaceSpec, nil).Once()
		s.jobService.On("KeepOnly", s.ctx, s.namespaceSpec, mock2.Anything, mock2.Anything).Return(errors.New("any error")).Once()
		stream.On("Send", mock2.Anything).Return(nil).Once()

		runtimeServiceServer := s.newRuntimeServiceServer()
		err := runtimeServiceServer.DeployJobSpecification(stream)

		s.Assert().Error(err)
		stream.AssertExpectations(s.T())
		s.namespaceService.AssertExpectations(s.T())
		s.jobService.AssertExpectations(s.T())
	})

	s.Run("JobServiceSyncError", func() {
		s.SetupTest()
		stream := new(mock.DeployJobSpecificationServer)
		stream.On("Context").Return(s.ctx)
		stream.On("Recv").Return(s.req, nil).Once()
		stream.On("Recv").Return(nil, io.EOF).Once()

		s.namespaceService.On("Get", s.ctx, s.req.GetProjectName(), s.req.GetNamespaceName()).Return(s.namespaceSpec, nil).Once()
		s.jobService.On("KeepOnly", s.ctx, s.namespaceSpec, mock2.Anything, mock2.Anything).Return(nil).Once()
		s.jobService.On("Sync", s.ctx, s.namespaceSpec, mock2.Anything).Return(errors.New("any error")).Once()
		stream.On("Send", mock2.Anything).Return(nil).Once()

		runtimeServiceServer := s.newRuntimeServiceServer()
		err := runtimeServiceServer.DeployJobSpecification(stream)

		s.Assert().Error(err)
		stream.AssertExpectations(s.T())
		s.namespaceService.AssertExpectations(s.T())
		s.jobService.AssertExpectations(s.T())
	})
}

// TODO: refactor to test suite
func TestJobSpecificationOnServer(t *testing.T) {
	log := log.NewNoop()
	ctx := context.Background()
	t.Run("GetJobSpecification", func(t *testing.T) {
		t.Run("should read a job spec", func(t *testing.T) {
			Version := "1.0.1"

			projectName := "a-data-project"
			jobName1 := "a-data-job"
			taskName := "a-data-task"

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

			execUnit1 := new(mock.BasePlugin)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: taskName,
			}, nil)
			defer execUnit1.AssertExpectations(t)

			jobSpecs := []models.JobSpec{
				{
					Name: jobName1,
					Task: models.JobSpecTask{
						Unit: &models.Plugin{
							Base: execUnit1,
						},
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
				},
			}

			allTasksRepo := new(mock.SupportedPluginRepo)
			allTasksRepo.On("GetByName", taskName).Return(execUnit1, nil)
			adapter := v1.NewAdapter(allTasksRepo, nil)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobSpecs[0].Name, namespaceSpec).Return(jobSpecs[0], nil)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				jobService,
				nil, nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			jobSpecAdapted, _ := adapter.ToJobProto(jobSpecs[0])
			deployRequest := pb.GetJobSpecificationRequest{ProjectName: projectName, JobName: jobSpecs[0].Name, NamespaceName: namespaceSpec.Name}
			jobSpecResp, err := runtimeServiceServer.GetJobSpecification(context.Background(), &deployRequest)
			assert.Nil(t, err)
			assert.Equal(t, jobSpecAdapted, jobSpecResp.Spec)
		})
	})

	t.Run("RegisterJobSpecification", func(t *testing.T) {
		t.Run("should save a job specification", func(t *testing.T) {
			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				Name: projectName,
				Config: map[string]string{
					"BUCKET": "gs://some_folder",
				},
			}

			namespaceSpec := models.NamespaceSpec{
				Name:        "dev-test-namespace-1",
				ProjectSpec: projectSpec,
			}

			jobName := "my-job"
			taskName := "bq2bq"
			execUnit1 := new(mock.BasePlugin)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:  taskName,
				Image: "random-image",
			}, nil)
			defer execUnit1.AssertExpectations(t)

			pluginRepo := new(mock.SupportedPluginRepo)
			pluginRepo.On("GetByName", taskName).Return(&models.Plugin{
				Base: execUnit1,
			}, nil)
			adapter := v1.NewAdapter(pluginRepo, nil)

			jobSpec := models.JobSpec{
				Name: jobName,
				Task: models.JobSpecTask{
					Unit: &models.Plugin{
						Base: execUnit1,
					},
					Config: models.JobSpecConfigs{
						{
							Name:  "DO",
							Value: "THIS",
						},
					},
					Window: models.JobSpecTaskWindow{
						Size:       time.Hour,
						Offset:     0,
						TruncateTo: "d",
					},
				},
				Assets: *models.JobAssets{}.New(
					[]models.JobSpecAsset{
						{
							Name:  "query.sql",
							Value: "select * from 1",
						},
					}),
				Dependencies: map[string]models.JobSpecDependency{},
			}

			jobSvc := new(mock.JobService)
			jobSvc.On("Create", ctx, namespaceSpec, jobSpec).Return(nil)
			jobSvc.On("Check", ctx, namespaceSpec, []models.JobSpec{jobSpec}, mock2.Anything).Return(nil)
			jobSvc.On("Sync", mock2.Anything, namespaceSpec, mock2.Anything).Return(nil)
			defer jobSvc.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"someVersion1.0",
				jobSvc,
				nil, nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			jobProto, _ := adapter.ToJobProto(jobSpec)
			request := pb.CreateJobSpecificationRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceSpec.Name,
				Spec:          jobProto,
			}
			resp, err := runtimeServiceServer.CreateJobSpecification(context.Background(), &request)
			assert.Nil(t, err)
			assert.Equal(t, &pb.CreateJobSpecificationResponse{
				Success: true,
				Message: "job my-job is created and deployed successfully on project a-data-project",
			}, resp)
		})
	})

	t.Run("DeleteJobSpecification", func(t *testing.T) {
		t.Run("should delete the job", func(t *testing.T) {
			Version := "1.0.1"

			projectName := "a-data-project"
			jobName1 := "a-data-job"
			taskName := "a-data-task"

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

			execUnit1 := new(mock.BasePlugin)
			defer execUnit1.AssertExpectations(t)

			jobSpecs := []models.JobSpec{
				{
					Name: jobName1,
					Task: models.JobSpecTask{
						Unit: &models.Plugin{
							Base: execUnit1,
						},
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
				},
			}

			pluginRepo := new(mock.SupportedPluginRepo)
			pluginRepo.On("GetByName", taskName).Return(&models.Plugin{
				Base: execUnit1,
			}, nil)
			adapter := v1.NewAdapter(pluginRepo, nil)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).
				Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			jobSpec := jobSpecs[0]

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobSpecs[0].Name, namespaceSpec).Return(jobSpecs[0], nil)
			jobService.On("Delete", mock2.Anything, namespaceSpec, jobSpec).Return(nil)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				jobService,
				nil, nil,
				nil,
				namespaceService,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			deployRequest := pb.DeleteJobSpecificationRequest{ProjectName: projectName, JobName: jobSpec.Name, NamespaceName: namespaceSpec.Name}
			resp, err := runtimeServiceServer.DeleteJobSpecification(ctx, &deployRequest)
			assert.Nil(t, err)
			assert.Equal(t, "job a-data-job has been deleted", resp.GetMessage())
		})
	})
}
