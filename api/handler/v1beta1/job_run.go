package v1beta1

import (
	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/service"
)

type JobRunServiceServer struct {
	jobSvc              models.JobService
	pluginRepo          models.PluginRepository
	projectService      service.ProjectService
	namespaceService    service.NamespaceService
	secretService       service.SecretService
	runSvc              service.JobRunService
	jobRunInputCompiler compiler.JobRunInputCompiler
	monitoringService   service.MonitoringService
	scheduler           models.SchedulerUnit
	l                   log.Logger
	pb.UnimplementedJobRunServiceServer
}

func NewJobRunServiceServer(l log.Logger, jobSvc models.JobService, projectService service.ProjectService, namespaceService service.NamespaceService, secretService service.SecretService, pluginRepo models.PluginRepository, instSvc service.JobRunService, jobRunInputCompiler compiler.JobRunInputCompiler, monitoringService service.MonitoringService, scheduler models.SchedulerUnit) *JobRunServiceServer {
	return &JobRunServiceServer{
		l:                   l,
		jobSvc:              jobSvc,
		pluginRepo:          pluginRepo,
		runSvc:              instSvc,
		jobRunInputCompiler: jobRunInputCompiler,
		scheduler:           scheduler,
		monitoringService:   monitoringService,
		namespaceService:    namespaceService,
		projectService:      projectService,
		secretService:       secretService,
	}
}
