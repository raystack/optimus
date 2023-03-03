package v1beta1

import (
	"context"
	"fmt"
	"strings"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

type ProjectHandler struct {
	l              log.Logger
	projectService ProjectService

	pb.UnimplementedProjectServiceServer
}

type ProjectService interface {
	Save(context.Context, *tenant.Project) error
	Get(context.Context, tenant.ProjectName) (*tenant.Project, error)
	GetAll(context.Context) ([]*tenant.Project, error)
}

type TenantService interface {
	GetDetails(ctx context.Context, tnnt tenant.Tenant) (*tenant.WithDetails, error)
	GetSecrets(ctx context.Context, tnnt tenant.Tenant) ([]*tenant.PlainTextSecret, error)
	GetSecret(ctx context.Context, tnnt tenant.Tenant, name string) (*tenant.PlainTextSecret, error)
}

func (ph *ProjectHandler) RegisterProject(ctx context.Context, req *pb.RegisterProjectRequest) (*pb.RegisterProjectResponse, error) {
	project, err := fromProjectProto(req.GetProject())
	if err != nil {
		return nil, errors.GRPCErr(err, fmt.Sprintf("not able to register project %s", req.GetProject().Name))
	}
	if err := ph.projectService.Save(ctx, project); err != nil {
		return nil, errors.GRPCErr(err, fmt.Sprintf("not able to register project %s", req.GetProject().Name))
	}

	// TODO update the proto to remove the success & Message
	return &pb.RegisterProjectResponse{}, nil
}

func (ph *ProjectHandler) ListProjects(ctx context.Context, _ *pb.ListProjectsRequest) (*pb.ListProjectsResponse, error) {
	projects, err := ph.projectService.GetAll(ctx)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to retrieve saved projects")
	}

	var projSpecsProto []*pb.ProjectSpecification
	for _, project := range projects {
		projSpecsProto = append(projSpecsProto, toProjectProto(project))
	}

	return &pb.ListProjectsResponse{
		Projects: projSpecsProto,
	}, nil
}

func (ph *ProjectHandler) GetProject(ctx context.Context, req *pb.GetProjectRequest) (*pb.GetProjectResponse, error) {
	projName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		return nil, errors.GRPCErr(err, fmt.Sprintf("failed to retrieve project [%s]", req.GetProjectName()))
	}
	project, err := ph.projectService.Get(ctx, projName)
	if err != nil {
		return nil, errors.GRPCErr(err, fmt.Sprintf("failed to retrieve project [%s]", req.GetProjectName()))
	}
	return &pb.GetProjectResponse{
		Project: toProjectProto(project),
	}, nil
}

func NewProjectHandler(l log.Logger, projectService ProjectService) *ProjectHandler {
	return &ProjectHandler{
		l:              l,
		projectService: projectService,
	}
}

func fromProjectProto(conf *pb.ProjectSpecification) (*tenant.Project, error) {
	pConf := map[string]string{}
	for key, val := range conf.GetConfig() {
		pConf[strings.ToUpper(key)] = val
	}

	return tenant.NewProject(conf.GetName(), pConf)
}

func toProjectProto(project *tenant.Project) *pb.ProjectSpecification {
	return &pb.ProjectSpecification{
		Name:   project.Name().String(),
		Config: project.GetConfigs(),
	}
}
