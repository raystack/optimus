package resolver_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/job_run"
	resolver "github.com/odpf/optimus/core/job_run/resolvers"
	"github.com/odpf/optimus/internal/lib/tree"
)

//
//// getDependencyObject - returns the dependency object by providing the specs and the dependency
//func getDependencyObject(specs map[string]job_run.JobWithDetails, dependencySpecs ...string) map[string]job_run.Upstreams {
//	dependenciesMap := make(map[string]job_run.JobUpstream)
//	for _, dependencySpec := range dependencySpecs {
//		depSpec, ok := specs[dependencySpec]
//		if !ok {
//			dependenciesMap[dependencySpec] = job_run.JobUpstream{}
//		}
//		dependenciesMap[dependencySpec] = job_run.JobUpstream{
//			JobName: depSpec.GetName(),
//		}
//	}
//	return dependenciesMap
//}

func getJobWithExternalUpstream(name string, upstreams ...*job_run.JobUpstream) *job_run.JobWithDetails {
	//var jobUpstreams []*job_run.JobUpstream
	//for _, n := range upstreams {
	//	jobUpstreams = append(jobUpstreams, &job_run.JobUpstream{
	//		JobName: n,
	//		Type:    job_run.UpstreamTypeExternal,
	//	})
	//}
	return &job_run.JobWithDetails{
		Name:      job_run.JobName(name),
		Upstreams: job_run.Upstreams{UpstreamJobs: upstreams},
	}
}

func getJobWithUpstream(name string, upstreams ...string) *job_run.JobWithDetails {
	var jobUpstreams []*job_run.JobUpstream
	for _, n := range upstreams {
		jobUpstreams = append(jobUpstreams, &job_run.JobUpstream{
			JobName: n,
		})
	}
	return &job_run.JobWithDetails{
		Name:      job_run.JobName(name),
		Upstreams: job_run.Upstreams{UpstreamJobs: jobUpstreams},
	}
}

func TestPriorityWeightResolver(t *testing.T) {
	//noDependency := job_run.Upstreams{}
	ctx := context.Background()

	t.Run("Resolve should assign correct weights to the DAGs with mentioned dependencies", func(t *testing.T) {
		spec1 := "dag1-no-deps"
		spec2 := "dag2-deps-on-dag1"
		spec3 := "dag3-deps-on-dag2"
		spec4 := "dag4-no-deps"
		spec5 := "dag5-deps-on-dag1"
		spec6 := "dag6-no-deps"
		spec7 := "dag7-deps-on-dag6"
		spec8 := "dag8-no-deps"
		spec9 := "dag9-deps-on-dag8"
		spec10 := "dag10-deps-on-dag9"
		spec11 := "dag11-deps-on-dag10"

		s1 := getJobWithUpstream(spec1)
		s2 := getJobWithUpstream(spec2, spec1)
		s3 := getJobWithUpstream(spec3, spec2)
		s4 := getJobWithUpstream(spec4)
		s5 := getJobWithUpstream(spec5, spec1)
		s6 := getJobWithUpstream(spec6)
		s7 := getJobWithUpstream(spec7, spec6)
		s8 := getJobWithUpstream(spec8)
		s9 := getJobWithUpstream(spec9, spec8)
		s10 := getJobWithUpstream(spec10, spec9)
		s11 := getJobWithUpstream(spec11, spec10)

		dagSpec := []*job_run.JobWithDetails{s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11}
		assigner := resolver.NewPriorityResolver()
		err := assigner.Resolve(ctx, dagSpec, nil)
		assert.Nil(t, err)

		max := resolver.MaxPriorityWeight
		max1 := max - resolver.PriorityWeightGap*1
		max2 := max - resolver.PriorityWeightGap*2
		max3 := max - resolver.PriorityWeightGap*3
		expectedWeights := map[job_run.JobName]int{
			s1.Name: max, s2.Name: max1, s3.Name: max2, s4.Name: max, s5.Name: max1,
			s6.Name: max, s7.Name: max1, s8.Name: max, s9.Name: max1, s10.Name: max2, s11.Name: max3,
		}

		for _, jobSpec := range dagSpec {
			assert.Equal(t, expectedWeights[jobSpec.Name], jobSpec.Priority)
		}
	})

	t.Run("Resolve should assign correct weights to the DAGs with mentioned dependencies", func(t *testing.T) {
		// run the test multiple times
		for i := 1; i < 10; i++ {

			spec1 := "dag1"
			spec11 := "dag1-1"
			spec12 := "dag1-2"
			spec111 := "dag1-1-1"
			spec112 := "dag1-1-2"
			spec121 := "dag1-2-1"
			spec122 := "dag1-2-2"
			s1 := getJobWithUpstream(spec1)
			s11 := getJobWithUpstream(spec11, spec1)
			s12 := getJobWithUpstream(spec12, spec1)
			s111 := getJobWithUpstream(spec111, spec11)
			s112 := getJobWithUpstream(spec112, spec12)
			s121 := getJobWithUpstream(spec121, spec12)
			s122 := getJobWithUpstream(spec122, spec12)

			spec2 := "dag2"
			spec21 := "dag2-1"
			spec22 := "dag2-2"
			spec211 := "dag2-1-1"
			spec212 := "dag2-1-2"
			spec221 := "dag2-2-1"
			spec222 := "dag2-2-2"
			s2 := getJobWithUpstream(spec2)
			s21 := getJobWithUpstream(spec21, spec2)
			s22 := getJobWithUpstream(spec22, spec2)
			s211 := getJobWithUpstream(spec211, spec21)
			s212 := getJobWithUpstream(spec212, spec21)
			s221 := getJobWithUpstream(spec221, spec22)
			s222 := getJobWithUpstream(spec222, spec22)

			dagSpec := []*job_run.JobWithDetails{s1, s11, s12, s111, s112, s121, s122, s2, s21, s22, s211, s212, s221, s222}

			assigner := resolver.NewPriorityResolver()
			err := assigner.Resolve(ctx, dagSpec, nil)
			assert.Nil(t, err)

			max := resolver.MaxPriorityWeight
			max1 := max - resolver.PriorityWeightGap*1
			max2 := max - resolver.PriorityWeightGap*2
			expectedWeights := map[string]int{
				spec1: max, spec11: max1, spec12: max1, spec111: max2, spec112: max2, spec121: max2, spec122: max2,
				spec2: max, spec21: max1, spec22: max1, spec211: max2, spec212: max2, spec221: max2, spec222: max2,
			}

			for _, jobSpec := range dagSpec {
				assert.Equal(t, expectedWeights[jobSpec.Name.String()], jobSpec.Priority)
			}
		}
	})

	t.Run("Resolve should assign correct weights to the DAGs with mentioned dependencies", func(t *testing.T) {
		spec1 := "dag1-no-deps"
		spec2 := "dag2-deps-on-dag1"
		spec3 := "dag3-deps-on-dag2"
		spec4 := "dag4-no-deps"
		spec5 := "dag5-deps-on-dag1"

		s1 := getJobWithUpstream(spec1)
		s2 := getJobWithUpstream(spec2, spec1)
		s3 := getJobWithUpstream(spec3, spec2)
		s4 := getJobWithUpstream(spec4)
		s5 := getJobWithUpstream(spec5, spec1)
		dagSpec := []*job_run.JobWithDetails{s1, s2, s3, s4, s5}
		assigner := resolver.NewPriorityResolver()
		err := assigner.Resolve(ctx, dagSpec, nil)
		assert.Nil(t, err)

		max := resolver.MaxPriorityWeight
		max1 := max - resolver.PriorityWeightGap*1
		max2 := max - resolver.PriorityWeightGap*2
		expectedWeights := map[string]int{spec1: max, spec2: max1, spec3: max2, spec4: max, spec5: max1}

		for _, jobSpec := range dagSpec {
			assert.Equal(t, expectedWeights[jobSpec.Name.String()], jobSpec.Priority)
		}
	})

	//t.Run("Resolve with a external tenant dependency should assign correct weights", func(t *testing.T) {
	//	spec1 := "dag1-no-deps"
	//	s1 := getJobWithUpstream(spec1)
	//
	//	spec2 := "dag2-deps-on-dag1"
	//	spec3 := "dag3-deps-on-dag2"
	//	spec4 := "dag4-no-deps"
	//	spec5 := "dag5-deps-on-dag1"
	//
	//	externalSpecName := "external-dag-dep"
	//	externalS1 := getJobWithUpstream(externalSpecName)
	//
	//	//dagSpec := []*job_run.JobWithDetails{s1}
	//
	//	// for the spec2, we'll add external spec as dependency
	//	// externalSpec := job_run.JobWithDetails{Name: externalSpecName, Upstreams: noDependency}
	//	//deps2 := getDependencyObject(specs, spec1)
	//	s2 := getJobWithExternalUpstream(spec1, &job_run.JobUpstream{
	//		JobName: externalSpecName,
	//		Type:    job_run.UpstreamTypeExternal,
	//		Tenant: tenant.Tenant{
	//			projName: tenant.ProjectName("external-project-name"),
	//		},
	//	})
	//
	//	specs[spec2] = job_run.JobWithDetails{Name: spec2, Upstreams: deps2}
	//	jobSpecs = append(jobSpecs, specs[spec2])
	//
	//	specs[spec3] = job_run.JobWithDetails{Name: spec3, Upstreams: getDependencyObject(specs, spec2)}
	//	jobSpecs = append(jobSpecs, specs[spec3])
	//
	//	specs[spec4] = job_run.JobWithDetails{Name: spec4, Upstreams: noDependency}
	//	jobSpecs = append(jobSpecs, specs[spec4])
	//
	//	specs[spec5] = job_run.JobWithDetails{Name: spec5, Upstreams: getDependencyObject(specs, spec1)}
	//	jobSpecs = append(jobSpecs, specs[spec5])
	//
	//	// for the spec2, we'll add external spec as dependency
	//	jobnameWithExternalDep := "job-with-1-external-dep"
	//	jobnameWithExternalDepDependencies := map[string]job_run.JobWithDetailsDependency{
	//		externalSpecName: {
	//			Job: &externalSpec, Project: &models.ProjectSpec{Name: "external-project-name"},
	//			Type: job_run.JobWithDetailsDependencyTypeInter,
	//		},
	//	}
	//	jobSpecs = append(jobSpecs, job_run.JobWithDetails{Name: jobnameWithExternalDep, Upstreams: jobnameWithExternalDepDependencies})
	//
	//	assigner := resolver.NewPriorityResolver()
	//	err := assigner.Resolve(ctx, jobSpecs, nil)
	//	assert.Nil(t, err)
	//
	//	max := resolver.MaxPriorityWeight
	//	max1 := max - resolver.PriorityWeightGap*1
	//	max2 := max - resolver.PriorityWeightGap*2
	//	expectedWeights := map[string]int{
	//		spec1: max, spec2: max1, spec3: max2, spec4: max, spec5: max1,
	//		jobnameWithExternalDep: max1,
	//	}
	//
	//	for _, jobSpec := range resolvedJobSpecs {
	//		assert.Equal(t, expectedWeights[jobSpec.Name], jobSpec.Task.Priority)
	//	}
	//})

	t.Run("Resolve should fail when circular dependency is detected (atleast one DAG with no dependency)", func(t *testing.T) {
		spec1 := "dag1-no-deps"
		spec2 := "dag2-deps-on-dag1"
		spec3 := "dag3-deps-on-dag2"

		s1 := getJobWithUpstream(spec1)
		s3 := getJobWithUpstream(spec3, spec2, spec1)
		s2 := getJobWithUpstream(spec2, spec3, spec1)

		dagSpec := []*job_run.JobWithDetails{s1, s2, s3}

		assigner := resolver.NewPriorityResolver()
		err := assigner.Resolve(ctx, dagSpec, nil)
		assert.Contains(t, err.Error(), "error occurred while resolving priority:")
		assert.Contains(t, err.Error(), tree.ErrCyclicDependencyEncountered.Error())
	})

	t.Run("resolve should give error on Cyclic dependency", func(t *testing.T) {
		spec2 := "dag2-deps-on-dag1"
		spec3 := "dag3-deps-on-dag2"

		s3 := getJobWithUpstream(spec3, spec2)
		s2 := getJobWithUpstream(spec2, spec3)

		dagSpec := []*job_run.JobWithDetails{s2, s3}

		assigner := resolver.NewPriorityResolver()
		err := assigner.Resolve(ctx, dagSpec, nil)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), tree.ErrCyclicDependencyEncountered.Error())
	})

	t.Run("Resolve should assign correct weights (maxWeight) with no dependencies", func(t *testing.T) {
		spec1 := "dag1-no-deps"
		spec2 := "dag4-no-deps"

		s1 := getJobWithUpstream(spec1)
		s2 := getJobWithUpstream(spec2)
		dagSpec := []*job_run.JobWithDetails{s1, s2}

		assigner := resolver.NewPriorityResolver()
		err := assigner.Resolve(ctx, dagSpec, nil)
		assert.Nil(t, err)

		max := resolver.MaxPriorityWeight

		for _, jobSpec := range dagSpec {
			assert.Equal(t, max, jobSpec.Priority)
		}
	})

	t.Run("Resolve should assign correct weight to single DAG", func(t *testing.T) {
		spec1 := "dag1-no-deps"
		s1 := getJobWithUpstream(spec1)
		dagSpec := []*job_run.JobWithDetails{s1}

		assigner := resolver.NewPriorityResolver()
		err := assigner.Resolve(ctx, dagSpec, nil)
		assert.Nil(t, err)

		for _, jobSpec := range dagSpec {
			assert.Equal(t, resolver.MaxPriorityWeight, jobSpec.Priority)
		}
	})
}

func TestDAGNode(t *testing.T) {
	t.Run("TreeNode should handle all TreeNode operations", func(t *testing.T) {
		dagSpec := job_run.JobWithDetails{Name: "testdag"}
		dagSpec2 := job_run.JobWithDetails{Name: "testdag"}
		dagSpec3 := job_run.JobWithDetails{Name: "testdag"}

		node := tree.NewTreeNode(dagSpec)
		node2 := tree.NewTreeNode(dagSpec2)
		node3 := tree.NewTreeNode(dagSpec3)

		assert.Equal(t, "testdag", node.GetName())
		assert.Equal(t, []*tree.TreeNode{}, node.Dependents)

		node.AddDependent(node2)
		assert.Equal(t, 1, len(node.Dependents))

		node.AddDependent(node3)
		assert.Equal(t, 2, len(node.Dependents))

		node2.AddDependent(node3)
		assert.Equal(t, 1, len(node2.Dependents))
		assert.Equal(t, 0, len(node3.Dependents))
	})
}

func TestMultiRootDAGTree(t *testing.T) {
	t.Run("should handle all NewMultiRootTree operations", func(t *testing.T) {
		dagSpec1 := job_run.JobWithDetails{Name: "testdag1"}
		dagSpec2 := job_run.JobWithDetails{Name: "testdag2"}
		dagSpec3 := job_run.JobWithDetails{Name: "testdag3"}

		node1 := tree.NewTreeNode(dagSpec1)
		node2 := tree.NewTreeNode(dagSpec2)
		node3 := tree.NewTreeNode(dagSpec3)
		node4 := tree.NewTreeNode(dagSpec2)

		node2.AddDependent(node3)
		node1.AddDependent(node1)

		dagTree := tree.NewMultiRootTree()

		// non-existing node should return nil, and ok=false
		n, ok := dagTree.GetNodeByName("non-existing")
		assert.False(t, ok)
		assert.Nil(t, n)

		// should return the node, when an existing node is requested by name
		dagTree.AddNode(node1)
		n, ok = dagTree.GetNodeByName(node1.GetName())
		assert.True(t, ok)
		assert.Equal(t, dagSpec1.Name.String(), n.GetName())
		assert.Equal(t, []*tree.TreeNode{}, dagTree.GetRootNodes())

		// should return root nodes, when added as root
		dagTree.MarkRoot(node1)
		assert.Equal(t, []*tree.TreeNode{node1}, dagTree.GetRootNodes())

		// adding nodes should maintain the dependency relationship
		dagTree.AddNode(node2)
		dagTree.AddNodeIfNotExist(node3)

		n, _ = dagTree.GetNodeByName(node1.GetName())
		assert.Equal(t, 1, len(n.Dependents))

		n, _ = dagTree.GetNodeByName(node2.GetName())
		assert.Equal(t, 1, len(n.Dependents))

		n, _ = dagTree.GetNodeByName(node3.GetName())
		assert.Equal(t, 0, len(n.Dependents))

		// AddNodeIfNotExist should not break the tree
		dagTree.AddNodeIfNotExist(node3)
		n, _ = dagTree.GetNodeByName(node3.GetName())
		assert.Equal(t, 0, len(n.Dependents))

		// AddNodeIfNotExist should not break the tree even when a new node
		// with same name is added
		dagTree.AddNodeIfNotExist(node4)
		n, _ = dagTree.GetNodeByName(node1.GetName())
		assert.Equal(t, 1, len(n.Dependents))

		// AddNode should break the tree if a node with same name is added
		// since node4 and node2 has same name. and node 4 has no deps.
		// it should replace node2 and break the tree
		dagTree.AddNode(node4)
		n, ok = dagTree.GetNodeByName(node2.GetName())
		assert.Equal(t, 0, len(n.Dependents))
		assert.Equal(t, true, ok)
	})

	t.Run("should detect any cycle in the tree", func(t *testing.T) {
		dagSpec1 := job_run.JobWithDetails{Name: "testdag1"}
		dagSpec2 := job_run.JobWithDetails{Name: "testdag2"}
		dagSpec3 := job_run.JobWithDetails{Name: "testdag3"}

		node1 := tree.NewTreeNode(dagSpec1)
		node2 := tree.NewTreeNode(dagSpec2)
		node3 := tree.NewTreeNode(dagSpec3)

		node1.AddDependent(node2)
		node2.AddDependent(node3)
		node3.AddDependent(node2)

		dagTree := tree.NewMultiRootTree()
		dagTree.AddNode(node1)
		dagTree.MarkRoot(node1)
		dagTree.AddNode(node2)
		dagTree.AddNode(node3)

		err := dagTree.ValidateCyclic()
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), tree.ErrCyclicDependencyEncountered.Error())
	})

	t.Run("should create tree with multi level dependencies", func(t *testing.T) {
		d1 := job_run.JobWithDetails{Name: "d1"}
		d11 := job_run.JobWithDetails{Name: "d11"}
		d12 := job_run.JobWithDetails{Name: "d12"}

		d111 := job_run.JobWithDetails{Name: "d111"}
		d112 := job_run.JobWithDetails{Name: "d112"}
		d121 := job_run.JobWithDetails{Name: "d121"}
		d122 := job_run.JobWithDetails{Name: "d122"}

		d1211 := job_run.JobWithDetails{Name: "d1211"}
		d1212 := job_run.JobWithDetails{Name: "d1212"}

		dagTree := tree.NewMultiRootTree()

		dagTree.AddNodeIfNotExist(tree.NewTreeNode(d1211))
		dagTree.AddNodeIfNotExist(tree.NewTreeNode(d1212))

		dagTree.AddNodeIfNotExist(tree.NewTreeNode(d11))
		dagTree.AddNodeIfNotExist(tree.NewTreeNode(d12))

		dagTree.AddNodeIfNotExist(tree.NewTreeNode(d111))
		dagTree.AddNodeIfNotExist(tree.NewTreeNode(d121))
		dagTree.AddNodeIfNotExist(tree.NewTreeNode(d122))

		node111, _ := dagTree.GetNodeByName(d111.Name.String())
		node112, _ := dagTree.GetNodeByName(d112.Name.String())
		if node112 == nil {
			node112 = tree.NewTreeNode(d112)
			dagTree.AddNode(tree.NewTreeNode(d112))
		}
		node121, _ := dagTree.GetNodeByName(d121.Name.String())
		node122, _ := dagTree.GetNodeByName(d122.Name.String())

		node11, _ := dagTree.GetNodeByName(d11.Name.String())
		node12, _ := dagTree.GetNodeByName(d12.Name.String())

		node1 := tree.NewTreeNode(d1)
		node1.AddDependent(node11).AddDependent(node12)
		dagTree.AddNode(node1)
		dagTree.MarkRoot(node1)

		node11.AddDependent(node111).AddDependent(node112)
		dagTree.AddNode(node11)

		node12.AddDependent(node121).AddDependent(node122)
		dagTree.AddNode(node12)

		node1211, _ := dagTree.GetNodeByName(d1211.Name.String())
		node1212, _ := dagTree.GetNodeByName(d1212.Name.String())
		node121.AddDependent(node1211).AddDependent(node1212)
		dagTree.AddNode(node121)
		dagTree.AddNode(node1211)
		dagTree.AddNode(node1212)

		err := dagTree.ValidateCyclic()
		assert.Nil(t, err)

		depsMap := map[*tree.TreeNode]int{
			node1:  2,
			node11: 2, node12: 2,
			node111: 0, node112: 0, node121: 2, node122: 0,
			node1211: 0, node1212: 0,
		}

		for node, depCount := range depsMap {
			n, ok := dagTree.GetNodeByName(node.GetName())
			assert.True(t, ok)
			assert.Equal(t, depCount, len(n.Dependents))
		}
	})

	t.Run("should not have cycles if only one node with no dependency is in the tree", func(t *testing.T) {
		dagSpec2 := job_run.JobWithDetails{Name: "testdag2"}
		node2 := tree.NewTreeNode(dagSpec2)

		dagTree := tree.NewMultiRootTree()
		dagTree.AddNode(node2)
		dagTree.MarkRoot(node2)

		err := dagTree.ValidateCyclic()
		assert.Nil(t, err)
	})

	t.Run("should not have cycles in a tree with no root", func(t *testing.T) {
		dagSpec2 := job_run.JobWithDetails{Name: "testdag2"}
		node2 := tree.NewTreeNode(dagSpec2)

		dagTree := tree.NewMultiRootTree()
		dagTree.AddNode(node2)

		err := dagTree.ValidateCyclic()
		assert.Nil(t, err)
	})

	t.Run("should detect any cycle in the tree with multiple sub trees", func(t *testing.T) {
		node1 := tree.NewTreeNode(job_run.JobWithDetails{Name: "testdag1"})
		node2 := tree.NewTreeNode(job_run.JobWithDetails{Name: "testdag2"})
		node3 := tree.NewTreeNode(job_run.JobWithDetails{Name: "testdag3"})
		node1.AddDependent(node2)
		node2.AddDependent(node3)

		node11 := tree.NewTreeNode(job_run.JobWithDetails{Name: "testdag11"})
		node21 := tree.NewTreeNode(job_run.JobWithDetails{Name: "testdag21"})
		node31 := tree.NewTreeNode(job_run.JobWithDetails{Name: "testdag31"})
		node41 := tree.NewTreeNode(job_run.JobWithDetails{Name: "testdag41"})
		node11.AddDependent(node21)
		node21.AddDependent(node31)
		node31.AddDependent(node11) // causing cyclic dep
		node31.AddDependent(node41)
		node41.AddDependent(node21)

		dagTree := tree.NewMultiRootTree()
		dagTree.AddNode(node1)
		dagTree.MarkRoot(node1)
		dagTree.AddNode(node2)
		dagTree.AddNode(node3)

		dagTree.AddNode(node11)
		dagTree.AddNode(node21)
		dagTree.MarkRoot(node21)
		dagTree.AddNode(node31)
		dagTree.AddNode(node41)

		err := dagTree.ValidateCyclic()
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), tree.ErrCyclicDependencyEncountered.Error())
	})
}
