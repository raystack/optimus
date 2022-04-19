package prime

import (
	"testing"

	"github.com/odpf/optimus/core/dag"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
)

func TestBuildGraph(t *testing.T) {
	uuid := utils.NewUUIDProvider()
	ns := models.NamespaceSpec{
		Name: "default",
		ProjectSpec: models.ProjectSpec{
			Name: "optimus-project",
		},
	}
	execUnit1 := new(mock.BasePlugin)
	execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name: "task1",
	}, nil)
	defer execUnit1.AssertExpectations(t)

	hookUnit1 := new(mock.BasePlugin)
	hookUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:     "hook1",
		HookType: models.HookTypePre,
	}, nil)

	hookUnit2 := new(mock.BasePlugin)
	hookUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:     "hook2",
		HookType: models.HookTypePre,
	}, nil)

	hookUnit3 := new(mock.BasePlugin)
	hookUnit3.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:     "hook3",
		HookType: models.HookTypePost,
	}, nil)

	testDag1 := dag.NewMultiRootDAG()
	testDag1.Connect(dag.BasicNode(ExecNode{
		name: "task1",
	}), dag.BasicNode(ExecNode{
		name: "hook1",
	}))

	testDag2 := dag.NewMultiRootDAG()
	// pre hook before task
	testDag2.Connect(dag.BasicNode(ExecNode{
		name: "task1",
	}), dag.BasicNode(ExecNode{
		name: "hook1",
	}))
	testDag2.Connect(dag.BasicNode(ExecNode{
		name: "task1",
	}), dag.BasicNode(ExecNode{
		name: "hook2",
	}))
	// intra hook
	testDag2.Connect(dag.BasicNode(ExecNode{
		name: "hook2",
	}), dag.BasicNode(ExecNode{
		name: "hook1",
	}))
	// task before post hook
	testDag2.Connect(dag.BasicNode(ExecNode{
		name: "hook3",
	}), dag.BasicNode(ExecNode{
		name: "task1",
	}))

	type args struct {
		uuid    utils.UUIDProvider
		ns      models.NamespaceSpec
		jobSpec models.JobSpec
	}
	tests := []struct {
		name    string
		args    args
		want    *dag.MultiRootDAG
		wantErr bool
	}{
		{
			name: "should build all nodes without dependencies",
			args: args{
				uuid: uuid,
				ns:   ns,
				jobSpec: models.JobSpec{
					Name: "job-1",
					Task: models.JobSpecTask{
						Unit: &models.Plugin{Base: execUnit1},
						Config: models.JobSpecConfigs{
							{
								Name:  "foo",
								Value: "bar",
							},
						},
					},
					Dependencies: nil,
					Assets:       models.JobAssets{},
					Hooks: []models.JobSpecHook{
						{
							Config:    nil,
							Unit:      &models.Plugin{Base: hookUnit1},
							DependsOn: nil,
						},
					},
				},
			},
			want:    testDag1,
			wantErr: false,
		},
		{
			name: "should build all nodes with hook dependencies",
			args: args{
				uuid: uuid,
				ns:   ns,
				jobSpec: models.JobSpec{
					Name: "job-2",
					Task: models.JobSpecTask{
						Unit: &models.Plugin{Base: execUnit1},
						Config: models.JobSpecConfigs{
							{
								Name:  "foo",
								Value: "bar",
							},
						},
					},
					Dependencies: nil,
					Assets:       models.JobAssets{},
					Hooks: []models.JobSpecHook{
						{
							Unit: &models.Plugin{Base: hookUnit1},
						},
						{
							Unit: &models.Plugin{Base: hookUnit2},
							DependsOn: []*models.JobSpecHook{
								{
									Unit: &models.Plugin{Base: hookUnit1},
								},
							},
						},
						{
							Unit: &models.Plugin{Base: hookUnit3},
						},
					},
				},
			},
			want:    testDag2,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildGraph(tt.args.uuid, tt.args.ns, tt.args.jobSpec)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildGraph() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got.Nodes()) != len(tt.want.Nodes()) {
				t.Errorf("BuildGraph() vertices want = %v, got %v", tt.want.Nodes(), got.Nodes())
				return
			}
			for _, wantEdge := range tt.want.Edges() {
				foundEdge := false
				for _, gotEdge := range got.Edges() {
					gotSourceName := gotEdge.Source().String()
					gotTargetName := gotEdge.Source().String()

					wantSourceName := wantEdge.Source().String()
					wantTargetName := wantEdge.Source().String()

					if gotSourceName == wantSourceName && gotTargetName == wantTargetName {
						foundEdge = true
					}
				}
				if !foundEdge {
					t.Errorf("BuildGraph() edge mismatch, unable to find s: %s to t: %s", wantEdge.Source().String(), wantEdge.Target().String())
					return
				}
			}
			for _, gotEdge := range got.Edges() {
				foundEdge := false
				for _, wantEdge := range tt.want.Edges() {
					gotSourceName := gotEdge.Source().String()
					gotTargetName := gotEdge.Source().String()

					wantSourceName := wantEdge.Source().String()
					wantTargetName := wantEdge.Source().String()

					if gotSourceName == wantSourceName && gotTargetName == wantTargetName {
						foundEdge = true
					}
				}
				if !foundEdge {
					t.Errorf("BuildGraph() edge mismatch, found extra edge s: %s to t: %s", gotEdge.Source().String(), gotEdge.Target().String())
					return
				}
			}
		})
	}
}
