package prime

import (
	"github.com/odpf/optimus/core/dag"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
)

type ExecNode struct {
	name    string
	request *models.ExecutorStartRequest
}

func (e ExecNode) GetName() string {
	return e.name
}

func (e ExecNode) GetRequest() *models.ExecutorStartRequest {
	return e.request
}

// BuildGraph creates a dependency graph of hooks and tasks.
// Each node in graph is made of ExecNode, extract data by casting
// node as: node.(ExecNode).GetRequest()
func BuildGraph(uuid utils.UUIDProvider, ns models.NamespaceSpec, jobSpec models.JobSpec) (*dag.MultiRootDAG, error) {
	gm := dag.NewMultiRootDAG()

	// build all exec nodes
	nodes := map[string]ExecNode{}
	taskUID, err := uuid.NewUUID()
	if err != nil {
		return gm, err
	}
	taskName := jobSpec.Task.Unit.Info().Name
	nodes[taskName] = ExecNode{
		name: taskName,
		request: &models.ExecutorStartRequest{
			ID:        taskUID.String(),
			Name:      taskName,
			Unit:      jobSpec.Task.Unit,
			Config:    jobSpec.Task.Config,
			Assets:    jobSpec.Assets,
			Namespace: ns,
			Type:      models.InstanceTypeTask,
		},
	}
	for _, taskHook := range jobSpec.Hooks {
		uid, err := uuid.NewUUID()
		if err != nil {
			return gm, err
		}
		hookName := taskHook.Unit.Info().Name
		nodes[hookName] = ExecNode{
			name: hookName,
			request: &models.ExecutorStartRequest{
				ID:        uid.String(),
				Name:      taskHook.Unit.Info().Name,
				Unit:      taskHook.Unit,
				Config:    taskHook.Config,
				Assets:    jobSpec.Assets,
				Namespace: ns,
				Type:      models.InstanceTypeHook,
			},
		}
	}

	// build exec dag
	// dependencies across hooks
	gm.AddNode(dag.BasicNode(nodes[taskName]))
	for _, taskHook := range jobSpec.Hooks {
		gm.AddNode(dag.BasicNode(nodes[taskHook.Unit.Info().Name]))

		if taskHook.Unit.Info().HookType == models.HookTypePre {
			gm.Connect(dag.BasicNode(nodes[jobSpec.Task.Unit.Info().Name]), dag.BasicNode(nodes[taskHook.Unit.Info().Name]))
		} else if taskHook.Unit.Info().HookType == models.HookTypePost {
			gm.Connect(dag.BasicNode(nodes[taskHook.Unit.Info().Name]), dag.BasicNode(nodes[jobSpec.Task.Unit.Info().Name]))
		}
		for _, dependentHook := range taskHook.DependsOn {
			gm.Connect(dag.BasicNode(nodes[taskHook.Unit.Info().Name]), dag.BasicNode(nodes[dependentHook.Unit.Info().Name]))
		}
	}
	gm.TransitiveReduction()
	return gm, nil
}
