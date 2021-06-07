package job_test

import (
	"strings"
	"testing"

	"github.com/odpf/optimus/core/multi_root_tree"

	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

// getDependencyObject - returns the dependency object by providing the specs and the dependency
func getDependencyObject(specs map[string]models.JobSpec, dependencySpecs ...string) map[string]models.JobSpecDependency {
	dependenciesMap := make(map[string]models.JobSpecDependency)
	for _, dependencySpec := range dependencySpecs {
		depSpec, ok := specs[dependencySpec]
		if !ok {
			dependenciesMap[dependencySpec] = models.JobSpecDependency{Job: nil}
		}
		dependenciesMap[dependencySpec] = models.JobSpecDependency{Job: &depSpec}
	}
	return dependenciesMap
}

func getMultiDependencyObject(specs map[string]models.JobSpec, dependencySpec1 string, dependencySpec2 string) map[string]models.JobSpecDependency {
	depSpec1 := specs[dependencySpec1]
	depSpec2 := specs[dependencySpec2]
	return map[string]models.JobSpecDependency{dependencySpec1: {Job: &depSpec1}, dependencySpec2: {Job: &depSpec2}}
}

func TestPriorityWeightResolver(t *testing.T) {
	noDependency := map[string]models.JobSpecDependency{}

	t.Run("Resolve should assign correct weights to the DAGs with mentioned dependencies", func(t *testing.T) {
		spec1 := "dag1-no-deps"
		spec2 := "dag2-deps-on-dag1"
		spec3 := "dag3-deps-on-dag2"

		spec5 := "dag5-deps-on-dag1"

		spec4 := "dag4-no-deps"

		spec6 := "dag6-no-deps"
		spec7 := "dag7-deps-on-dag6"

		spec8 := "dag8-no-deps"
		spec9 := "dag9-deps-on-dag8"
		spec10 := "dag10-deps-on-dag9"
		spec11 := "dag11-deps-on-dag10"

		var (
			specs   = make(map[string]models.JobSpec)
			dagSpec = make([]models.JobSpec, 0)
		)

		specs[spec1] = models.JobSpec{Name: spec1, Dependencies: noDependency}
		dagSpec = append(dagSpec, specs[spec1])
		specs[spec2] = models.JobSpec{Name: spec2, Dependencies: getDependencyObject(specs, spec1)}
		dagSpec = append(dagSpec, specs[spec2])
		specs[spec3] = models.JobSpec{Name: spec3, Dependencies: getDependencyObject(specs, spec2)}
		dagSpec = append(dagSpec, specs[spec3])

		specs[spec4] = models.JobSpec{Name: spec4, Dependencies: noDependency}
		dagSpec = append(dagSpec, specs[spec4])

		specs[spec5] = models.JobSpec{Name: spec5, Dependencies: getDependencyObject(specs, spec1)}
		dagSpec = append(dagSpec, specs[spec5])

		specs[spec6] = models.JobSpec{Name: spec6, Dependencies: noDependency}
		dagSpec = append(dagSpec, specs[spec6])

		specs[spec7] = models.JobSpec{Name: spec7, Dependencies: getDependencyObject(specs, spec6)}
		dagSpec = append(dagSpec, specs[spec7])

		specs[spec8] = models.JobSpec{Name: spec8, Dependencies: noDependency}
		dagSpec = append(dagSpec, specs[spec8])

		specs[spec9] = models.JobSpec{Name: spec9, Dependencies: getDependencyObject(specs, spec8)}
		dagSpec = append(dagSpec, specs[spec9])

		specs[spec10] = models.JobSpec{Name: spec10, Dependencies: getDependencyObject(specs, spec9)}
		dagSpec = append(dagSpec, specs[spec10])

		specs[spec10] = models.JobSpec{Name: spec10, Dependencies: getDependencyObject(specs, spec9)}
		dagSpec = append(dagSpec, specs[spec10])

		specs[spec11] = models.JobSpec{Name: spec11, Dependencies: getDependencyObject(specs, spec10)}
		dagSpec = append(dagSpec, specs[spec11])

		assginer := job.NewPriorityResolver()
		resolvedJobSpecs, err := assginer.Resolve(dagSpec)
		assert.Nil(t, err)

		max := job.MaxPriorityWeight
		max_1 := max - job.PriorityWeightGap*1
		max_2 := max - job.PriorityWeightGap*2
		max_3 := max - job.PriorityWeightGap*3
		expectedWeights := map[string]int{
			spec1: max, spec2: max_1, spec3: max_2, spec4: max, spec5: max_1,
			spec6: max, spec7: max_1, spec8: max, spec9: max_1, spec10: max_2, spec11: max_3,
		}

		for _, jobSpec := range resolvedJobSpecs {
			assert.Equal(t, expectedWeights[jobSpec.Name], jobSpec.Task.Priority)
		}
	})

	t.Run("Resolve should assign correct weights to the DAGs with mentioned dependencies", func(t *testing.T) {
		// run the test multiple times
		for i := 1; i < 10; i++ {
			var (
				specs   = make(map[string]models.JobSpec)
				dagSpec = make([]models.JobSpec, 0)
			)

			spec1 := "dag1"
			spec11 := "dag1-1"
			spec12 := "dag1-2"
			spec111 := "dag1-1-1"
			spec112 := "dag1-1-2"
			spec121 := "dag1-2-1"
			spec122 := "dag1-2-2"
			specs[spec1] = models.JobSpec{Name: spec1, Dependencies: noDependency}
			dagSpec = append(dagSpec, specs[spec1])
			specs[spec11] = models.JobSpec{Name: spec11, Dependencies: getDependencyObject(specs, spec1)}
			dagSpec = append(dagSpec, specs[spec11])
			specs[spec12] = models.JobSpec{Name: spec12, Dependencies: getDependencyObject(specs, spec1)}
			dagSpec = append(dagSpec, specs[spec12])
			specs[spec111] = models.JobSpec{Name: spec111, Dependencies: getDependencyObject(specs, spec11)}
			dagSpec = append(dagSpec, specs[spec111])
			specs[spec112] = models.JobSpec{Name: spec112, Dependencies: getDependencyObject(specs, spec11)}
			dagSpec = append(dagSpec, specs[spec112])
			specs[spec121] = models.JobSpec{Name: spec121, Dependencies: getDependencyObject(specs, spec12)}
			dagSpec = append(dagSpec, specs[spec121])
			specs[spec122] = models.JobSpec{Name: spec122, Dependencies: getDependencyObject(specs, spec12)}
			dagSpec = append(dagSpec, specs[spec122])

			spec2 := "dag2"
			spec21 := "dag2-1"
			spec22 := "dag2-2"
			spec211 := "dag2-1-1"
			spec212 := "dag2-1-2"
			spec221 := "dag2-2-1"
			spec222 := "dag2-2-2"
			specs[spec2] = models.JobSpec{Name: spec2, Dependencies: noDependency}
			dagSpec = append(dagSpec, specs[spec2])
			specs[spec21] = models.JobSpec{Name: spec21, Dependencies: getDependencyObject(specs, spec2)}
			dagSpec = append(dagSpec, specs[spec21])
			specs[spec22] = models.JobSpec{Name: spec22, Dependencies: getDependencyObject(specs, spec2)}
			dagSpec = append(dagSpec, specs[spec22])
			specs[spec211] = models.JobSpec{Name: spec211, Dependencies: getDependencyObject(specs, spec21)}
			dagSpec = append(dagSpec, specs[spec211])
			specs[spec212] = models.JobSpec{Name: spec212, Dependencies: getDependencyObject(specs, spec21)}
			dagSpec = append(dagSpec, specs[spec212])
			specs[spec221] = models.JobSpec{Name: spec221, Dependencies: getDependencyObject(specs, spec22)}
			dagSpec = append(dagSpec, specs[spec221])
			specs[spec222] = models.JobSpec{Name: spec222, Dependencies: getDependencyObject(specs, spec22)}
			dagSpec = append(dagSpec, specs[spec222])

			assginer := job.NewPriorityResolver()
			resolvedJobSpecs, err := assginer.Resolve(dagSpec)
			assert.Nil(t, err)

			max := job.MaxPriorityWeight
			max_1 := max - job.PriorityWeightGap*1
			max_2 := max - job.PriorityWeightGap*2
			expectedWeights := map[string]int{
				spec1: max, spec11: max_1, spec12: max_1, spec111: max_2, spec112: max_2, spec121: max_2, spec122: max_2,
				spec2: max, spec21: max_1, spec22: max_1, spec211: max_2, spec212: max_2, spec221: max_2, spec222: max_2,
			}

			for _, jobSpec := range resolvedJobSpecs {
				assert.Equal(t, expectedWeights[jobSpec.Name], jobSpec.Task.Priority)
			}
		}
	})

	t.Run("Resolve should assign correct weights to the DAGs with mentioned dependencies", func(t *testing.T) {
		spec1 := "dag1-no-deps"
		spec2 := "dag2-deps-on-dag1"
		spec3 := "dag3-deps-on-dag2"
		spec4 := "dag4-no-deps"
		spec5 := "dag5-deps-on-dag1"

		var (
			specs   = make(map[string]models.JobSpec)
			dagSpec = make([]models.JobSpec, 0)
		)

		specs[spec1] = models.JobSpec{Name: spec1, Dependencies: noDependency}
		dagSpec = append(dagSpec, specs[spec1])

		specs[spec2] = models.JobSpec{Name: spec2, Dependencies: getDependencyObject(specs, spec1)}
		dagSpec = append(dagSpec, specs[spec2])

		specs[spec3] = models.JobSpec{Name: spec3, Dependencies: getDependencyObject(specs, spec2)}
		dagSpec = append(dagSpec, specs[spec3])

		specs[spec4] = models.JobSpec{Name: spec4, Dependencies: noDependency}
		dagSpec = append(dagSpec, specs[spec4])

		specs[spec5] = models.JobSpec{Name: spec5, Dependencies: getDependencyObject(specs, spec1)}
		dagSpec = append(dagSpec, specs[spec5])

		assginer := job.NewPriorityResolver()
		resolvedJobSpecs, err := assginer.Resolve(dagSpec)
		assert.Nil(t, err)

		max := job.MaxPriorityWeight
		max_1 := max - job.PriorityWeightGap*1
		max_2 := max - job.PriorityWeightGap*2
		expectedWeights := map[string]int{spec1: max, spec2: max_1, spec3: max_2, spec4: max, spec5: max_1}

		for _, jobSpec := range resolvedJobSpecs {
			assert.Equal(t, expectedWeights[jobSpec.Name], jobSpec.Task.Priority)
		}
	})

	t.Run("Resolve with a external tenant dependency should assign correct weights", func(t *testing.T) {
		spec1 := "dag1-no-deps"
		spec2 := "dag2-deps-on-dag1"
		spec3 := "dag3-deps-on-dag2"
		spec4 := "dag4-no-deps"
		spec5 := "dag5-deps-on-dag1"

		var (
			specs    = make(map[string]models.JobSpec)
			jobSpecs = make([]models.JobSpec, 0)
		)

		specs[spec1] = models.JobSpec{Name: spec1, Dependencies: noDependency}
		jobSpecs = append(jobSpecs, specs[spec1])

		// for the spec2, we'll add external spec as dependency
		externalSpecName := "external-dag-dep"
		externalSpec := models.JobSpec{Name: externalSpecName, Dependencies: noDependency}
		deps2 := getDependencyObject(specs, spec1)
		deps2[externalSpecName] = models.JobSpecDependency{Job: &externalSpec, Project: &models.ProjectSpec{Name: "external-project-name"},
			Type: models.JobSpecDependencyTypeInter}
		specs[spec2] = models.JobSpec{Name: spec2, Dependencies: deps2}
		jobSpecs = append(jobSpecs, specs[spec2])

		specs[spec3] = models.JobSpec{Name: spec3, Dependencies: getDependencyObject(specs, spec2)}
		jobSpecs = append(jobSpecs, specs[spec3])

		specs[spec4] = models.JobSpec{Name: spec4, Dependencies: noDependency}
		jobSpecs = append(jobSpecs, specs[spec4])

		specs[spec5] = models.JobSpec{Name: spec5, Dependencies: getDependencyObject(specs, spec1)}
		jobSpecs = append(jobSpecs, specs[spec5])

		// for the spec2, we'll add external spec as dependency
		jobnameWithExternalDep := "job-with-1-external-dep"
		jobnameWithExternalDepDependencies := map[string]models.JobSpecDependency{
			externalSpecName: {Job: &externalSpec, Project: &models.ProjectSpec{Name: "external-project-name"},
				Type: models.JobSpecDependencyTypeInter},
		}
		jobSpecs = append(jobSpecs, models.JobSpec{Name: jobnameWithExternalDep, Dependencies: jobnameWithExternalDepDependencies})

		assginer := job.NewPriorityResolver()
		resolvedJobSpecs, err := assginer.Resolve(jobSpecs)
		assert.Nil(t, err)

		max := job.MaxPriorityWeight
		max_1 := max - job.PriorityWeightGap*1
		max_2 := max - job.PriorityWeightGap*2
		expectedWeights := map[string]int{spec1: max, spec2: max_1, spec3: max_2, spec4: max, spec5: max_1,
			jobnameWithExternalDep: max_1}

		for _, jobSpec := range resolvedJobSpecs {
			assert.Equal(t, expectedWeights[jobSpec.Name], jobSpec.Task.Priority)
		}
	})

	t.Run("Resolve should fail when circular dependency is detected (atleast one DAG with no dependency)", func(t *testing.T) {
		spec1 := "dag1-no-deps"
		spec2 := "dag2-deps-on-dag1"
		spec3 := "dag3-deps-on-dag2"

		var (
			specs   = make(map[string]models.JobSpec)
			dagSpec = make([]models.JobSpec, 0)
		)

		specs[spec1] = models.JobSpec{Name: spec1, Dependencies: noDependency}
		specs[spec2] = models.JobSpec{Name: spec2}
		specs[spec3] = models.JobSpec{Name: spec3}

		s3 := specs[spec3]
		s3.Dependencies = getMultiDependencyObject(specs, spec2, spec1)
		specs[spec3] = s3

		s2 := specs[spec2]
		s2.Dependencies = getMultiDependencyObject(specs, spec3, spec1)
		specs[spec2] = s2

		s3 = specs[spec3]
		s3.Dependencies = getMultiDependencyObject(specs, spec2, spec1)
		specs[spec3] = s3

		dagSpec = append(dagSpec, specs[spec1])
		dagSpec = append(dagSpec, specs[spec2])
		dagSpec = append(dagSpec, specs[spec3])

		assginer := job.NewPriorityResolver()
		_, err := assginer.Resolve(dagSpec)
		assert.Contains(t, err.Error(), "error occurred while resolving priority:")
		assert.Contains(t, err.Error(), multi_root_tree.ErrCyclicDependencyEncountered.Error())
	})

	t.Run("Resolve should give minWeight when all DAGs are dependent on each other", func(t *testing.T) {
		spec2 := "dag2-deps-on-dag1"
		spec3 := "dag3-deps-on-dag2"

		var (
			specs   = make(map[string]models.JobSpec)
			dagSpec = make([]models.JobSpec, 0)
		)

		specs[spec2] = models.JobSpec{Name: spec2}
		specs[spec3] = models.JobSpec{Name: spec3}

		s3 := specs[spec3]
		s3.Dependencies = getDependencyObject(specs, spec2)
		specs[spec3] = s3

		s2 := specs[spec2]
		s2.Dependencies = getDependencyObject(specs, spec3)
		specs[spec2] = s2

		dagSpec = append(dagSpec, specs[spec2])
		dagSpec = append(dagSpec, specs[spec3])

		assginer := job.NewPriorityResolver()
		_, err := assginer.Resolve(dagSpec)
		assert.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), multi_root_tree.ErrCyclicDependencyEncountered.Error()))
	})

	t.Run("Resolve should assign correct weights (maxWeight) with no dependencies", func(t *testing.T) {
		spec1 := "dag1-no-deps"
		spec4 := "dag4-no-deps"

		var (
			specs   = make(map[string]models.JobSpec)
			dagSpec = make([]models.JobSpec, 0)
		)

		specs[spec1] = models.JobSpec{Name: spec1, Dependencies: noDependency}
		dagSpec = append(dagSpec, specs[spec1])

		specs[spec4] = models.JobSpec{Name: spec4, Dependencies: noDependency}
		dagSpec = append(dagSpec, specs[spec4])

		assginer := job.NewPriorityResolver()
		resolvedJobSpecs, err := assginer.Resolve(dagSpec)
		assert.Nil(t, err)

		max := job.MaxPriorityWeight
		expectedWeights := map[string]int{spec1: max, spec4: max}

		for _, jobSpec := range resolvedJobSpecs {
			assert.Equal(t, expectedWeights[jobSpec.Name], jobSpec.Task.Priority)
		}
	})

	t.Run("Resolve should assign correct weight to single DAG", func(t *testing.T) {
		spec1 := "dag1-no-deps"
		var (
			specs   = make(map[string]models.JobSpec)
			dagSpec = make([]models.JobSpec, 0)
		)

		specs[spec1] = models.JobSpec{Name: spec1, Dependencies: noDependency}
		dagSpec = append(dagSpec, specs[spec1])

		assginer := job.NewPriorityResolver()
		resolvedJobSpecs, err := assginer.Resolve(dagSpec)
		assert.Nil(t, err)

		max := job.MaxPriorityWeight
		expectedWeights := map[string]int{spec1: max}

		for _, jobSpec := range resolvedJobSpecs {
			assert.Equal(t, expectedWeights[jobSpec.Name], jobSpec.Task.Priority)
		}
	})

	t.Run("Resolve should minWeight when weight for a non existing DAG is requested", func(t *testing.T) {
		spec1 := "dag1-no-deps"
		spec2 := "dag2-non-existing"

		var (
			specs   = make(map[string]models.JobSpec)
			dagSpec = make([]models.JobSpec, 0)
		)

		specs[spec1] = models.JobSpec{Name: spec1, Dependencies: noDependency}
		dagSpec = append(dagSpec, specs[spec1])

		specs[spec2] = models.JobSpec{Name: spec2, Dependencies: getDependencyObject(specs, spec1)}

		assginer := job.NewPriorityResolver()
		resolvedJobSpecs, err := assginer.Resolve(dagSpec)
		assert.Nil(t, err)

		expectedWeights := map[string]int{spec1: job.MaxPriorityWeight, spec2: job.MinPriorityWeight}
		for _, jobSpec := range resolvedJobSpecs {
			assert.Equal(t, expectedWeights[jobSpec.Name], jobSpec.Task.Priority)
		}
	})
}

func TestDAGNode(t *testing.T) {
	t.Run("TreeNode should handle all TreeNode operations", func(t *testing.T) {
		dagSpec := models.JobSpec{Name: "testdag"}
		dagSpec2 := models.JobSpec{Name: "testdag"}
		dagSpec3 := models.JobSpec{Name: "testdag"}

		node := multi_root_tree.NewTreeNode(dagSpec)
		node2 := multi_root_tree.NewTreeNode(dagSpec2)
		node3 := multi_root_tree.NewTreeNode(dagSpec3)

		assert.Equal(t, "testdag", node.GetName())
		assert.Equal(t, []*multi_root_tree.TreeNode{}, node.Dependents)

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
	t.Run("should handle all NewMultiRootDAGTree operations", func(t *testing.T) {
		dagSpec1 := models.JobSpec{Name: "testdag1"}
		dagSpec2 := models.JobSpec{Name: "testdag2"}
		dagSpec3 := models.JobSpec{Name: "testdag3"}

		node1 := multi_root_tree.NewTreeNode(dagSpec1)
		node2 := multi_root_tree.NewTreeNode(dagSpec2)
		node3 := multi_root_tree.NewTreeNode(dagSpec3)
		node4 := multi_root_tree.NewTreeNode(dagSpec2)

		node2.AddDependent(node3)
		node1.AddDependent(node1)

		tree := multi_root_tree.NewMultiRootDAGTree()

		// non-existing node should return nil, and ok=false
		n, ok := tree.GetNodeByName("non-existing")
		assert.False(t, ok)
		assert.Nil(t, n)

		// should return the node, when an existing node is requested by name
		tree.AddNode(node1)
		n, ok = tree.GetNodeByName(node1.GetName())
		assert.True(t, ok)
		assert.Equal(t, dagSpec1.Name, n.GetName())
		assert.Equal(t, []*multi_root_tree.TreeNode{}, tree.GetRootNodes())

		// should return root nodes, when added as root
		tree.MarkRoot(node1)
		assert.Equal(t, []*multi_root_tree.TreeNode{node1}, tree.GetRootNodes())

		// adding nodes should maintain the dependency relationship
		tree.AddNode(node2)
		tree.AddNodeIfNotExist(node3)

		n, _ = tree.GetNodeByName(node1.GetName())
		assert.Equal(t, 1, len(n.Dependents))

		n, _ = tree.GetNodeByName(node2.GetName())
		assert.Equal(t, 1, len(n.Dependents))

		n, _ = tree.GetNodeByName(node3.GetName())
		assert.Equal(t, 0, len(n.Dependents))

		// AddNodeIfNotExist should not break the tree
		tree.AddNodeIfNotExist(node3)
		n, _ = tree.GetNodeByName(node3.GetName())
		assert.Equal(t, 0, len(n.Dependents))

		// AddNodeIfNotExist should not break the tree even when a new node
		// with same name is added
		tree.AddNodeIfNotExist(node4)
		n, _ = tree.GetNodeByName(node1.GetName())
		assert.Equal(t, 1, len(n.Dependents))

		// AddNode should break the tree if a node with same name is added
		// since node4 and node2 has same name. and node 4 has no deps.
		// it should replace node2 and break the tree
		tree.AddNode(node4)
		n, ok = tree.GetNodeByName(node2.GetName())
		assert.Equal(t, 0, len(n.Dependents))
		assert.Equal(t, true, ok)
	})

	t.Run("should detect any cycle in the tree", func(t *testing.T) {
		dagSpec1 := models.JobSpec{Name: "testdag1"}
		dagSpec2 := models.JobSpec{Name: "testdag2"}
		dagSpec3 := models.JobSpec{Name: "testdag3"}

		node1 := multi_root_tree.NewTreeNode(dagSpec1)
		node2 := multi_root_tree.NewTreeNode(dagSpec2)
		node3 := multi_root_tree.NewTreeNode(dagSpec3)

		node1.AddDependent(node2)
		node2.AddDependent(node3)
		node3.AddDependent(node2)

		tree := multi_root_tree.NewMultiRootDAGTree()
		tree.AddNode(node1)
		tree.MarkRoot(node1)
		tree.AddNode(node2)
		tree.AddNode(node3)

		err := tree.IsCyclic()
		assert.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), multi_root_tree.ErrCyclicDependencyEncountered.Error()))
	})

	t.Run("should create tree with multi level dependencies", func(t *testing.T) {
		d1 := models.JobSpec{Name: "d1"}
		d11 := models.JobSpec{Name: "d11"}
		d12 := models.JobSpec{Name: "d12"}

		d111 := models.JobSpec{Name: "d111"}
		d112 := models.JobSpec{Name: "d112"}
		d121 := models.JobSpec{Name: "d121"}
		d122 := models.JobSpec{Name: "d122"}

		d1211 := models.JobSpec{Name: "d1211"}
		d1212 := models.JobSpec{Name: "d1212"}

		tree := multi_root_tree.NewMultiRootDAGTree()

		tree.AddNodeIfNotExist(multi_root_tree.NewTreeNode(d1211))
		tree.AddNodeIfNotExist(multi_root_tree.NewTreeNode(d1212))

		tree.AddNodeIfNotExist(multi_root_tree.NewTreeNode(d11))
		tree.AddNodeIfNotExist(multi_root_tree.NewTreeNode(d12))

		tree.AddNodeIfNotExist(multi_root_tree.NewTreeNode(d111))
		tree.AddNodeIfNotExist(multi_root_tree.NewTreeNode(d121))
		tree.AddNodeIfNotExist(multi_root_tree.NewTreeNode(d122))

		node111, _ := tree.GetNodeByName(d111.Name)
		node112, _ := tree.GetNodeByName(d112.Name)
		if node112 == nil {
			node112 = multi_root_tree.NewTreeNode(d112)
			tree.AddNode(multi_root_tree.NewTreeNode(d112))
		}
		node121, _ := tree.GetNodeByName(d121.Name)
		node122, _ := tree.GetNodeByName(d122.Name)

		node11, _ := tree.GetNodeByName(d11.Name)
		node12, _ := tree.GetNodeByName(d12.Name)

		node1 := multi_root_tree.NewTreeNode(d1)
		node1.AddDependent(node11).AddDependent(node12)
		tree.AddNode(node1)
		tree.MarkRoot(node1)

		node11.AddDependent(node111).AddDependent(node112)
		tree.AddNode(node11)

		node12.AddDependent(node121).AddDependent(node122)
		tree.AddNode(node12)

		node1211, _ := tree.GetNodeByName(d1211.Name)
		node1212, _ := tree.GetNodeByName(d1212.Name)
		node121.AddDependent(node1211).AddDependent(node1212)
		tree.AddNode(node121)
		tree.AddNode(node1211)
		tree.AddNode(node1212)

		err := tree.IsCyclic()
		assert.Nil(t, err)

		depsMap := map[*multi_root_tree.TreeNode]int{
			node1:  2,
			node11: 2, node12: 2,
			node111: 0, node112: 0, node121: 2, node122: 0,
			node1211: 0, node1212: 0,
		}

		for node, depCount := range depsMap {
			n, ok := tree.GetNodeByName(node.GetName())
			assert.True(t, ok)
			assert.Equal(t, depCount, len(n.Dependents))
		}
	})

	t.Run("should not have cycles if only one node with no dependency is in the tree", func(t *testing.T) {
		dagSpec2 := models.JobSpec{Name: "testdag2"}
		node2 := multi_root_tree.NewTreeNode(dagSpec2)

		tree := multi_root_tree.NewMultiRootDAGTree()
		tree.AddNode(node2)
		tree.MarkRoot(node2)

		err := tree.IsCyclic()
		assert.Nil(t, err)
	})

	t.Run("should not have cycles in a tree with no root", func(t *testing.T) {
		dagSpec2 := models.JobSpec{Name: "testdag2"}
		node2 := multi_root_tree.NewTreeNode(dagSpec2)

		tree := multi_root_tree.NewMultiRootDAGTree()
		tree.AddNode(node2)

		err := tree.IsCyclic()
		assert.Nil(t, err)
	})

	t.Run("should detect any cycle in the tree with multiple sub trees", func(t *testing.T) {
		node1 := multi_root_tree.NewTreeNode(models.JobSpec{Name: "testdag1"})
		node2 := multi_root_tree.NewTreeNode(models.JobSpec{Name: "testdag2"})
		node3 := multi_root_tree.NewTreeNode(models.JobSpec{Name: "testdag3"})
		node1.AddDependent(node2)
		node2.AddDependent(node3)

		node11 := multi_root_tree.NewTreeNode(models.JobSpec{Name: "testdag11"})
		node21 := multi_root_tree.NewTreeNode(models.JobSpec{Name: "testdag21"})
		node31 := multi_root_tree.NewTreeNode(models.JobSpec{Name: "testdag31"})
		node41 := multi_root_tree.NewTreeNode(models.JobSpec{Name: "testdag41"})
		node11.AddDependent(node21)
		node21.AddDependent(node31)
		node31.AddDependent(node11) // causing cyclic dep
		node31.AddDependent(node41)
		node41.AddDependent(node21)

		tree := multi_root_tree.NewMultiRootDAGTree()
		tree.AddNode(node1)
		tree.MarkRoot(node1)
		tree.AddNode(node2)
		tree.AddNode(node3)

		tree.AddNode(node11)
		tree.AddNode(node21)
		tree.MarkRoot(node21)
		tree.AddNode(node31)
		tree.AddNode(node41)

		err := tree.IsCyclic()
		assert.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), multi_root_tree.ErrCyclicDependencyEncountered.Error()))
	})
}
