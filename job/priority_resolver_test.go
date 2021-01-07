package job_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
)

func TestPriorityWeightResolver(t *testing.T) {
	// t.Run("GetByDAG should assign correct weights to the DAGs with mentioned dependencies", func(t *testing.T) {
	// 	spec1 := "dag1-no-deps"
	// 	spec2 := "dag2-deps-on-dag1"
	// 	spec3 := "dag3-deps-on-dag2"

	// 	spec5 := "dag5-deps-on-dag1"

	// 	spec4 := "dag4-no-deps"

	// 	spec6 := "dag6-no-deps"
	// 	spec7 := "dag7-deps-on-dag6"

	// 	spec8 := "dag8-no-deps"
	// 	spec9 := "dag9-deps-on-dag8"
	// 	spec10 := "dag10-deps-on-dag9"
	// 	spec11 := "dag11-deps-on-dag10"

	// 	var (
	// 		specs   = make(map[string]models.JobSpec)
	// 		dagSpec = make([]models.JobSpec, 0)
	// 	)

	// 	noDependency := map[string]models.JobSpecDependency{}
	// 	specs[spec1] = models.JobSpec{Name: spec1, Dependencies: noDependency}
	// 	dagSpec = append(dagSpec, specs[spec1])

	// 	x1 := specs[spec1]
	// 	x2 := specs[spec2]
	// 	x6 := specs[spec6]
	// 	x8 := specs[spec8]
	// 	x9 := specs[spec9]
	// 	x10 := specs[spec10]

	// 	specs[spec2] = models.JobSpec{Name: spec2, Dependencies: map[string]models.JobSpecDependency{spec1: {Job: &x1}}}
	// 	dagSpec = append(dagSpec, specs[spec2])

	// 	specs[spec3] = models.JobSpec{Name: spec3, Dependencies: map[string]models.JobSpecDependency{spec2: {Job: &x2}}}
	// 	dagSpec = append(dagSpec, specs[spec3])

	// 	specs[spec4] = models.JobSpec{Name: spec4, Dependencies: noDependency}
	// 	dagSpec = append(dagSpec, specs[spec4])

	// 	specs[spec5] = models.JobSpec{Name: spec5, Dependencies: map[string]models.JobSpecDependency{spec1: {Job: &x1}}}
	// 	dagSpec = append(dagSpec, specs[spec5])

	// 	specs[spec6] = models.JobSpec{Name: spec6, Dependencies: noDependency}
	// 	dagSpec = append(dagSpec, specs[spec6])

	// 	specs[spec7] = models.JobSpec{Name: spec7, Dependencies: map[string]models.JobSpecDependency{spec6: {Job: &x6}}}
	// 	dagSpec = append(dagSpec, specs[spec7])

	// 	specs[spec8] = models.JobSpec{Name: spec8, Dependencies: noDependency}
	// 	dagSpec = append(dagSpec, specs[spec8])
	// 	specs[spec9] = models.JobSpec{Name: spec9, Dependencies: map[string]models.JobSpecDependency{spec8: {Job: &x8}}}
	// 	dagSpec = append(dagSpec, specs[spec9])
	// 	specs[spec10] = models.JobSpec{Name: spec10, Dependencies: map[string]models.JobSpecDependency{spec9: {Job: &x9}}}
	// 	dagSpec = append(dagSpec, specs[spec10])
	// 	specs[spec10] = models.JobSpec{Name: spec10, Dependencies: map[string]models.JobSpecDependency{spec9: {Job: &x9}}}
	// 	dagSpec = append(dagSpec, specs[spec10])
	// 	specs[spec11] = models.JobSpec{Name: spec11, Dependencies: map[string]models.JobSpecDependency{spec10: {Job: &x10}}}
	// 	dagSpec = append(dagSpec, specs[spec11])

	// 	dagSpecRepo := new(mock.JobSpecRepository)
	// 	dagSpecRepo.On("GetAll").Return(dagSpec, nil)
	// 	defer dagSpecRepo.AssertExpectations(t)

	// 	resolv := new(mock.DependencyResolver)
	// 	resolv.On("Resolve", dagSpec).Return(dagSpec, nil)
	// 	defer resolv.AssertExpectations(t)

	// 	assginer := job.NewPriorityResolver(dagSpecRepo, resolv)
	// 	max := job.MaxPriorityWeight
	// 	max_1 := max - job.PriorityWeightGap*1
	// 	max_2 := max - job.PriorityWeightGap*2
	// 	max_3 := max - job.PriorityWeightGap*3
	// 	expectedWeights := map[string]int{
	// 		spec1: max, spec2: max_1, spec3: max_2, spec4: max, spec5: max_1,
	// 		spec6: max, spec7: max_1, spec8: max, spec9: max_1, spec10: max_2, spec11: max_3,
	// 	}
	// 	for specName, expectedWeight := range expectedWeights {
	// 		calculatedWeight, err := assginer.GetByDAG(specs[specName])
	// 		assert.Nil(t, err)
	// 		assert.Equal(t, expectedWeight, calculatedWeight)
	// 	}
	// })

	// t.Run("GetByDAG should assign correct weights to the DAGs with mentioned dependencies", func(t *testing.T) {
	// 	// run the test multiple times
	// 	for i := 1; i < 10; i++ {
	// 		var (
	// 			specs   = make(map[string]models.JobSpec)
	// 			dagSpec = make([]models.JobSpec, 0)
	// 		)

	// 		spec1 := "dag1"
	// 		spec11 := "dag1-1"
	// 		spec12 := "dag1-2"
	// 		spec111 := "dag1-1-1"
	// 		spec112 := "dag1-1-2"
	// 		spec121 := "dag1-2-1"
	// 		spec122 := "dag1-2-2"
	// 		specs[spec1] = models.JobSpec{Name: spec1, Dependencies: []models.DAGDependency{}}
	// 		dagSpec = append(dagSpec, specs[spec1])
	// 		specs[spec11] = models.JobSpec{Name: spec11, Dependencies: []models.DAGDependency{{DAGID: spec1}}}
	// 		dagSpec = append(dagSpec, specs[spec11])
	// 		specs[spec12] = models.JobSpec{Name: spec12, Dependencies: []models.DAGDependency{{DAGID: spec1}}}
	// 		dagSpec = append(dagSpec, specs[spec12])
	// 		specs[spec111] = models.JobSpec{Name: spec111, Dependencies: []models.DAGDependency{{DAGID: spec11}}}
	// 		dagSpec = append(dagSpec, specs[spec111])
	// 		specs[spec112] = models.JobSpec{Name: spec112, Dependencies: []models.DAGDependency{{DAGID: spec11}}}
	// 		dagSpec = append(dagSpec, specs[spec112])
	// 		specs[spec121] = models.JobSpec{Name: spec121, Dependencies: []models.DAGDependency{{DAGID: spec12}}}
	// 		dagSpec = append(dagSpec, specs[spec121])
	// 		specs[spec122] = models.JobSpec{Name: spec122, Dependencies: []models.DAGDependency{{DAGID: spec12}}}
	// 		dagSpec = append(dagSpec, specs[spec122])

	// 		spec2 := "dag2"
	// 		spec21 := "dag2-1"
	// 		spec22 := "dag2-2"
	// 		spec211 := "dag2-1-1"
	// 		spec212 := "dag2-1-2"
	// 		spec221 := "dag2-2-1"
	// 		spec222 := "dag2-2-2"
	// 		specs[spec2] = models.JobSpec{Name: spec2, Dependencies: []models.DAGDependency{}}
	// 		dagSpec = append(dagSpec, specs[spec2])
	// 		specs[spec21] = models.JobSpec{Name: spec21, Dependencies: []models.DAGDependency{{DAGID: spec2}}}
	// 		dagSpec = append(dagSpec, specs[spec21])
	// 		specs[spec22] = models.JobSpec{Name: spec22, Dependencies: []models.DAGDependency{{DAGID: spec2}}}
	// 		dagSpec = append(dagSpec, specs[spec22])
	// 		specs[spec211] = models.JobSpec{Name: spec211, Dependencies: []models.DAGDependency{{DAGID: spec21}}}
	// 		dagSpec = append(dagSpec, specs[spec211])
	// 		specs[spec212] = models.JobSpec{Name: spec212, Dependencies: []models.DAGDependency{{DAGID: spec21}}}
	// 		dagSpec = append(dagSpec, specs[spec212])
	// 		specs[spec221] = models.JobSpec{Name: spec221, Dependencies: []models.DAGDependency{{DAGID: spec22}}}
	// 		dagSpec = append(dagSpec, specs[spec221])
	// 		specs[spec222] = models.JobSpec{Name: spec222, Dependencies: []models.DAGDependency{{DAGID: spec22}}}
	// 		dagSpec = append(dagSpec, specs[spec222])

	// 		dagSpecRepo := new(mock.DAGSpecRepository)
	// 		dagSpecRepo.On("GetAll").Return(dagSpec, nil)
	// 		defer dagSpecRepo.AssertExpectations(t)

	// 		resolv := new(mock.DAGResolver)
	// 		defer resolv.AssertExpectations(t)
	// 		for _, spec := range specs {
	// 			resolv.On("MergeDependencies", spec).Return(spec.Dependencies, nil)
	// 		}

	// 		assginer := dag.NewPriorityWeightResolver(dagSpecRepo, resolv)
	// 		assginer.GetByDAG(specs[spec1])
	// 		max := dag.MaxPriorityWeight
	// 		max_1 := max - dag.PriorityWeightGap*1
	// 		max_2 := max - dag.PriorityWeightGap*2
	// 		expectedWeights := map[string]int{
	// 			spec1: max, spec11: max_1, spec12: max_1, spec111: max_2, spec112: max_2, spec121: max_2, spec122: max_2,
	// 			spec2: max, spec21: max_1, spec22: max_1, spec211: max_2, spec212: max_2, spec221: max_2, spec222: max_2,
	// 		}
	// 		for specName, expectedWeight := range expectedWeights {
	// 			calculatedWeight, err := assginer.GetByDAG(specs[specName])
	// 			assert.Nil(t, err)
	// 			assert.Equal(t, expectedWeight, calculatedWeight)
	// 		}
	// 	}
	// })

	// t.Run("GetByDAG should assign correct weights to the DAGs with mentioned dependencies", func(t *testing.T) {
	// 	spec1 := "dag1-no-deps"
	// 	spec2 := "dag2-deps-on-dag1"
	// 	spec3 := "dag3-deps-on-dag2"
	// 	spec4 := "dag4-no-deps"
	// 	spec5 := "dag5-deps-on-dag1"

	// 	var (
	// 		specs   = make(map[string]models.JobSpec)
	// 		dagSpec = make([]models.JobSpec, 0)
	// 	)

	// 	specs[spec1] = models.JobSpec{Name: spec1, Dependencies: []models.DAGDependency{}}
	// 	dagSpec = append(dagSpec, specs[spec1])

	// 	specs[spec2] = models.JobSpec{Name: spec2, Dependencies: []models.DAGDependency{{DAGID: spec1}}}
	// 	dagSpec = append(dagSpec, specs[spec2])

	// 	specs[spec3] = models.JobSpec{Name: spec3, Dependencies: []models.DAGDependency{{DAGID: spec2}}}
	// 	dagSpec = append(dagSpec, specs[spec3])

	// 	specs[spec4] = models.JobSpec{Name: spec4, Dependencies: []models.DAGDependency{}}
	// 	dagSpec = append(dagSpec, specs[spec4])

	// 	specs[spec5] = models.JobSpec{Name: spec5, Dependencies: []models.DAGDependency{{DAGID: spec1}}}
	// 	dagSpec = append(dagSpec, specs[spec5])

	// 	dagSpecRepo := new(mock.DAGSpecRepository)
	// 	dagSpecRepo.On("GetAll").Return(dagSpec, nil)
	// 	defer dagSpecRepo.AssertExpectations(t)

	// 	resolv := new(mock.DAGResolver)
	// 	defer resolv.AssertExpectations(t)
	// 	for _, spec := range specs {
	// 		resolv.On("MergeDependencies", spec).Return(spec.Dependencies, nil)
	// 	}

	// 	assginer := dag.NewPriorityWeightResolver(dagSpecRepo, resolv)

	// 	max := dag.MaxPriorityWeight
	// 	max_1 := max - dag.PriorityWeightGap*1
	// 	max_2 := max - dag.PriorityWeightGap*2
	// 	expectedWeights := map[string]int{spec1: max, spec2: max_1, spec3: max_2, spec4: max, spec5: max_1}
	// 	for specName, expectedWeight := range expectedWeights {
	// 		calculatedWeight, err := assginer.GetByDAG(specs[specName])
	// 		assert.Nil(t, err)
	// 		assert.Equal(t, expectedWeight, calculatedWeight)
	// 	}
	// })

	// t.Run("GetByDAG should fail when circular dependency is detected (atleast one DAG with no dependency)", func(t *testing.T) {
	// 	spec1 := "dag1-no-deps"
	// 	spec2 := "dag2-deps-on-dag1"
	// 	spec3 := "dag3-deps-on-dag2"

	// 	var (
	// 		specs   = make(map[string]models.JobSpec)
	// 		dagSpec = make([]models.JobSpec, 0)
	// 	)

	// 	specs[spec1] = models.JobSpec{Name: spec1, Dependencies: []models.DAGDependency{}}
	// 	dagSpec = append(dagSpec, specs[spec1])

	// 	specs[spec2] = models.JobSpec{Name: spec2, Dependencies: []models.DAGDependency{{DAGID: spec3}, {DAGID: spec1}}}
	// 	dagSpec = append(dagSpec, specs[spec2])

	// 	specs[spec3] = models.JobSpec{Name: spec3, Dependencies: []models.DAGDependency{{DAGID: spec2}}}
	// 	dagSpec = append(dagSpec, specs[spec3])

	// 	dagSpecRepo := new(mock.DAGSpecRepository)
	// 	dagSpecRepo.On("GetAll").Return(dagSpec, nil)
	// 	defer dagSpecRepo.AssertExpectations(t)

	// 	resolv := new(mock.DAGResolver)
	// 	defer resolv.AssertExpectations(t)
	// 	for _, spec := range specs {
	// 		resolv.On("MergeDependencies", spec).Return(spec.Dependencies, nil)
	// 	}

	// 	assginer := dag.NewPriorityWeightResolver(dagSpecRepo, resolv)
	// 	for _, dagSpec := range specs {
	// 		v, err := assginer.GetByDAG(dagSpec)
	// 		assert.Equal(t, dag.MinPriorityWeight, v)
	// 		assert.Equal(t, err.Error(), dag.ErrCyclicDependencyEncountered)
	// 	}
	// })

	// t.Run("GetByDAG should give minWeight when all DAGs are dependent on each other", func(t *testing.T) {
	// 	spec1 := "dag1-deps-on-dag3"
	// 	spec2 := "dag2-deps-on-dag1"
	// 	spec3 := "dag3-deps-on-dag2"

	// 	var (
	// 		specs   = make(map[string]models.JobSpec)
	// 		dagSpec = make([]models.JobSpec, 0)
	// 	)

	// 	specs[spec1] = models.JobSpec{Name: spec1, Dependencies: []models.DAGDependency{{DAGID: spec2}}}
	// 	dagSpec = append(dagSpec, specs[spec1])

	// 	specs[spec2] = models.JobSpec{Name: spec2, Dependencies: []models.DAGDependency{{DAGID: spec3}}}
	// 	dagSpec = append(dagSpec, specs[spec2])

	// 	specs[spec3] = models.JobSpec{Name: spec3, Dependencies: []models.DAGDependency{{DAGID: spec1}}}
	// 	dagSpec = append(dagSpec, specs[spec3])

	// 	dagSpecRepo := new(mock.DAGSpecRepository)
	// 	dagSpecRepo.On("GetAll").Return(dagSpec, nil)
	// 	defer dagSpecRepo.AssertExpectations(t)

	// 	resolv := new(mock.DAGResolver)
	// 	defer resolv.AssertExpectations(t)
	// 	for _, spec := range specs {
	// 		resolv.On("MergeDependencies", spec).Return(spec.Dependencies, nil)
	// 	}

	// 	assginer := dag.NewPriorityWeightResolver(dagSpecRepo, resolv)

	// 	for _, dagSpec := range specs {
	// 		v, err := assginer.GetByDAG(dagSpec)
	// 		assert.Equal(t, dag.MinPriorityWeight, v)
	// 		assert.Nil(t, err)
	// 	}
	// })

	// t.Run("GetByDAG should assign correct weights (maxWeight) with no dependencies", func(t *testing.T) {
	// 	spec1 := "dag1-no-deps"
	// 	spec4 := "dag4-no-deps"

	// 	var (
	// 		specs   = make(map[string]models.JobSpec)
	// 		dagSpec = make([]models.JobSpec, 0)
	// 	)

	// 	specs[spec1] = models.JobSpec{Name: spec1, Dependencies: []models.DAGDependency{}}
	// 	dagSpec = append(dagSpec, specs[spec1])

	// 	specs[spec4] = models.JobSpec{Name: spec4, Dependencies: []models.DAGDependency{}}
	// 	dagSpec = append(dagSpec, specs[spec4])

	// 	dagSpecRepo := new(mock.DAGSpecRepository)
	// 	dagSpecRepo.On("GetAll").Return(dagSpec, nil)
	// 	defer dagSpecRepo.AssertExpectations(t)

	// 	resolv := new(mock.DAGResolver)
	// 	defer resolv.AssertExpectations(t)
	// 	for _, spec := range specs {
	// 		resolv.On("MergeDependencies", spec).Return(spec.Dependencies, nil)
	// 	}

	// 	assginer := dag.NewPriorityWeightResolver(dagSpecRepo, resolv)
	// 	max := dag.MaxPriorityWeight
	// 	expectedWeights := map[string]int{spec1: max, spec4: max}
	// 	for specName, expectedWeight := range expectedWeights {
	// 		calculatedWeight, err := assginer.GetByDAG(specs[specName])
	// 		assert.Nil(t, err)
	// 		assert.Equal(t, expectedWeight, calculatedWeight)
	// 	}
	// })

	// t.Run("GetByDAG should assign correct weight to single DAG", func(t *testing.T) {
	// 	spec1 := "dag1-no-deps"
	// 	var (
	// 		specs   = make(map[string]models.JobSpec)
	// 		dagSpec = make([]models.JobSpec, 0)
	// 	)

	// 	specs[spec1] = models.JobSpec{Name: spec1, Dependencies: []models.DAGDependency{}}
	// 	dagSpec = append(dagSpec, specs[spec1])

	// 	dagSpecRepo := new(mock.DAGSpecRepository)
	// 	dagSpecRepo.On("GetAll").Return(dagSpec, nil)
	// 	defer dagSpecRepo.AssertExpectations(t)

	// 	resolv := new(mock.DAGResolver)
	// 	defer resolv.AssertExpectations(t)
	// 	for _, spec := range specs {
	// 		resolv.On("MergeDependencies", spec).Return(spec.Dependencies, nil)
	// 	}

	// 	assginer := dag.NewPriorityWeightResolver(dagSpecRepo, resolv)
	// 	max := dag.MaxPriorityWeight
	// 	expectedWeights := map[string]int{spec1: max}
	// 	for specName, expectedWeight := range expectedWeights {
	// 		calculatedWeight, err := assginer.GetByDAG(specs[specName])
	// 		assert.Nil(t, err)
	// 		assert.Equal(t, expectedWeight, calculatedWeight)
	// 	}
	// })

	// t.Run("GetByDAG should return error is GetAll fails", func(t *testing.T) {
	// 	spec1 := "dag1-no-deps"
	// 	spec2 := "dag2-deps-on-dag1"
	// 	spec3 := "dag3-deps-on-dag2"

	// 	var (
	// 		specs   = make(map[string]models.JobSpec)
	// 		dagSpec = make([]models.JobSpec, 0)
	// 	)

	// 	specs[spec1] = models.JobSpec{Name: spec1, Dependencies: []models.DAGDependency{}}
	// 	dagSpec = append(dagSpec, specs[spec1])

	// 	specs[spec2] = models.JobSpec{Name: spec2, Dependencies: []models.DAGDependency{{DAGID: spec1}}}
	// 	dagSpec = append(dagSpec, specs[spec2])

	// 	specs[spec3] = models.JobSpec{Name: spec3, Dependencies: []models.DAGDependency{{DAGID: spec2}}}
	// 	dagSpec = append(dagSpec, specs[spec3])

	// 	dagSpecRepo := new(mock.DAGSpecRepository)
	// 	dagSpecRepo.On("GetAll").Return(nil, errors.New("a random error"))
	// 	defer dagSpecRepo.AssertExpectations(t)

	// 	resolv := new(mock.DAGResolver)
	// 	defer resolv.AssertExpectations(t)

	// 	assginer := dag.NewPriorityWeightResolver(dagSpecRepo, resolv)

	// 	computedWeights, err := assginer.GetByDAG(specs[spec1])
	// 	assert.Equal(t, err.Error(), "a random error")
	// 	assert.Equal(t, dag.MinPriorityWeight, computedWeights)
	// })

	// t.Run("GetByDAG should return error if a dependent DAG was not found", func(t *testing.T) {
	// 	spec1 := "dag1-no-deps"
	// 	spec2 := "dag2-deps-on-dag1"
	// 	spec3 := "dag3-deps-on-dag2"

	// 	var (
	// 		specs   = make(map[string]models.JobSpec)
	// 		dagSpec = make([]models.JobSpec, 0)
	// 	)

	// 	specs[spec1] = models.JobSpec{Name: spec1, Dependencies: []models.DAGDependency{}}
	// 	dagSpec = append(dagSpec, specs[spec1])

	// 	specs[spec3] = models.JobSpec{Name: spec3, Dependencies: []models.DAGDependency{{DAGID: spec2}, {DAGID: spec1}}}
	// 	dagSpec = append(dagSpec, specs[spec3])

	// 	dagSpecRepo := new(mock.DAGSpecRepository)
	// 	dagSpecRepo.On("GetAll").Return(dagSpec, nil)
	// 	defer dagSpecRepo.AssertExpectations(t)

	// 	resolv := new(mock.DAGResolver)
	// 	defer resolv.AssertExpectations(t)
	// 	for _, spec := range specs {
	// 		resolv.On("MergeDependencies", spec).Return(spec.Dependencies, nil)
	// 	}

	// 	assginer := dag.NewPriorityWeightResolver(dagSpecRepo, resolv)

	// 	computedWeights, err := assginer.GetByDAG(specs[spec1])
	// 	assert.Equal(t, err.Error(), fmt.Sprintf(dag.ErrDAGSpecNotFound, spec2))
	// 	assert.Equal(t, dag.MinPriorityWeight, computedWeights)
	// })

	// t.Run("GetByDAG should minWeight when weight for a non existing DAG is requested", func(t *testing.T) {
	// 	spec1 := "dag1-no-deps"
	// 	spec2 := "dag2-non-existing"

	// 	var (
	// 		specs   = make(map[string]models.JobSpec)
	// 		dagSpec = make([]models.JobSpec, 0)
	// 	)

	// 	specs[spec1] = models.JobSpec{Name: spec1, Dependencies: []models.DAGDependency{}}
	// 	dagSpec = append(dagSpec, specs[spec1])

	// 	specs[spec2] = models.JobSpec{Name: spec2, Dependencies: []models.DAGDependency{{DAGID: spec1}}}

	// 	dagSpecRepo := new(mock.DAGSpecRepository)
	// 	dagSpecRepo.On("GetAll").Return(dagSpec, nil)
	// 	defer dagSpecRepo.AssertExpectations(t)

	// 	resolv := new(mock.DAGResolver)
	// 	defer resolv.AssertExpectations(t)
	// 	resolv.On("MergeDependencies", specs[spec1]).Return(specs[spec1].Dependencies, nil)

	// 	assginer := dag.NewPriorityWeightResolver(dagSpecRepo, resolv)

	// 	computedWeights, err := assginer.GetByDAG(specs[spec1])
	// 	assert.Nil(t, err)
	// 	assert.Equal(t, dag.MaxPriorityWeight, computedWeights)

	// 	computedWeights, err = assginer.GetByDAG(specs[spec2])
	// 	assert.Nil(t, err)
	// 	assert.Equal(t, dag.MinPriorityWeight, computedWeights)
	// })
}

func TestDAGNode(t *testing.T) {
	t.Run("DAGNode should handle all DAGNode operations", func(t *testing.T) {
		dagSpec := models.JobSpec{Name: "testdag"}
		dagSpec2 := models.JobSpec{Name: "testdag"}
		dagSpec3 := models.JobSpec{Name: "testdag"}

		node := job.NewDAGNode(dagSpec)
		node2 := job.NewDAGNode(dagSpec2)
		node3 := job.NewDAGNode(dagSpec3)

		assert.Equal(t, "testdag", node.GetName())
		assert.Equal(t, []*job.DAGNode([]*job.DAGNode{}), node.Dependents)

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

		node1 := job.NewDAGNode(dagSpec1)
		node2 := job.NewDAGNode(dagSpec2)
		node3 := job.NewDAGNode(dagSpec3)
		node4 := job.NewDAGNode(dagSpec2)

		node2.AddDependent(node3)
		node1.AddDependent(node1)

		tree := job.NewMultiRootDAGTree()

		// non-existing node should return nil, and ok=false
		n, ok := tree.GetNodeByName("non-existing")
		assert.False(t, ok)
		assert.Nil(t, n)

		// should return the node, when an existing node is requested by name
		tree.AddNode(node1)
		n, ok = tree.GetNodeByName(node1.GetName())
		assert.True(t, ok)
		assert.Equal(t, dagSpec1.Name, n.GetName())
		assert.Equal(t, []*job.DAGNode{}, tree.GetRootNodes())

		// should return root nodes, when added as root
		tree.SetRoot(node1)
		assert.Equal(t, []*job.DAGNode{node1}, tree.GetRootNodes())

		// adding nodes should maintain the dependency relationship
		tree.AddNode(node2)
		tree.AddNodeIfNotExist(node3)

		n, ok = tree.GetNodeByName(node1.GetName())
		assert.Equal(t, 1, len(n.Dependents))

		n, ok = tree.GetNodeByName(node2.GetName())
		assert.Equal(t, 1, len(n.Dependents))

		n, ok = tree.GetNodeByName(node3.GetName())
		assert.Equal(t, 0, len(n.Dependents))

		// AddNodeIfNotExist should not break the tree
		tree.AddNodeIfNotExist(node3)
		n, ok = tree.GetNodeByName(node3.GetName())
		assert.Equal(t, 0, len(n.Dependents))

		// AddNodeIfNotExist should not break the tree even when a new node
		// with same name is added
		tree.AddNodeIfNotExist(node4)
		n, ok = tree.GetNodeByName(node1.GetName())
		assert.Equal(t, 1, len(n.Dependents))

		// AddNode should break the tree if a node with same name is added
		// since node4 and node2 has same name. and node 4 has no deps.
		// it should replace node2 and break the tree
		tree.AddNode(node4)
		n, ok = tree.GetNodeByName(node2.GetName())
		assert.Equal(t, 0, len(n.Dependents))
	})

	t.Run("should detect any cycle in the tree", func(t *testing.T) {
		dagSpec1 := models.JobSpec{Name: "testdag1"}
		dagSpec2 := models.JobSpec{Name: "testdag2"}
		dagSpec3 := models.JobSpec{Name: "testdag3"}

		node1 := job.NewDAGNode(dagSpec1)
		node2 := job.NewDAGNode(dagSpec2)
		node3 := job.NewDAGNode(dagSpec3)

		node1.AddDependent(node2)
		node2.AddDependent(node3)
		node3.AddDependent(node2)

		tree := job.NewMultiRootDAGTree()
		tree.AddNode(node1)
		tree.SetRoot(node1)
		tree.AddNode(node2)
		tree.AddNode(node3)

		err := tree.IsCyclic()
		assert.Equal(t, job.ErrCyclicDependencyEncountered, err.Error())
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

		tree := job.NewMultiRootDAGTree()

		tree.AddNodeIfNotExist(job.NewDAGNode(d1211))
		tree.AddNodeIfNotExist(job.NewDAGNode(d1212))

		tree.AddNodeIfNotExist(job.NewDAGNode(d11))
		tree.AddNodeIfNotExist(job.NewDAGNode(d12))

		tree.AddNodeIfNotExist(job.NewDAGNode(d111))
		tree.AddNodeIfNotExist(job.NewDAGNode(d121))
		tree.AddNodeIfNotExist(job.NewDAGNode(d122))

		node111, _ := tree.GetNodeByName(d111.Name)
		node112, _ := tree.GetNodeByName(d112.Name)
		if node112 == nil {
			node112 = job.NewDAGNode(d112)
			tree.AddNode(job.NewDAGNode(d112))
		}
		node121, _ := tree.GetNodeByName(d121.Name)
		node122, _ := tree.GetNodeByName(d122.Name)

		node11, _ := tree.GetNodeByName(d11.Name)
		node12, _ := tree.GetNodeByName(d12.Name)

		node1 := job.NewDAGNode(d1)
		node1.AddDependent(node11).AddDependent(node12)
		tree.AddNode(node1)
		tree.SetRoot(node1)

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

		depsMap := map[*job.DAGNode]int{
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
		node2 := job.NewDAGNode(dagSpec2)

		tree := job.NewMultiRootDAGTree()
		tree.AddNode(node2)
		tree.SetRoot(node2)

		err := tree.IsCyclic()
		assert.Nil(t, err)
	})

	t.Run("should not have cycles in a tree with no root", func(t *testing.T) {
		dagSpec2 := models.JobSpec{Name: "testdag2"}
		node2 := job.NewDAGNode(dagSpec2)

		tree := job.NewMultiRootDAGTree()
		tree.AddNode(node2)

		err := tree.IsCyclic()
		assert.Nil(t, err)
	})

	t.Run("should detect any cycle in the tree with multiple sub trees", func(t *testing.T) {
		node1 := job.NewDAGNode(models.JobSpec{Name: "testdag1"})
		node2 := job.NewDAGNode(models.JobSpec{Name: "testdag2"})
		node3 := job.NewDAGNode(models.JobSpec{Name: "testdag3"})
		node1.AddDependent(node2)
		node2.AddDependent(node3)

		node11 := job.NewDAGNode(models.JobSpec{Name: "testdag11"})
		node21 := job.NewDAGNode(models.JobSpec{Name: "testdag21"})
		node31 := job.NewDAGNode(models.JobSpec{Name: "testdag31"})
		node41 := job.NewDAGNode(models.JobSpec{Name: "testdag41"})
		node11.AddDependent(node21)
		node21.AddDependent(node31)
		node31.AddDependent(node11) // causing cyclic dep
		node31.AddDependent(node41)
		node41.AddDependent(node21)

		tree := job.NewMultiRootDAGTree()
		tree.AddNode(node1)
		tree.SetRoot(node1)
		tree.AddNode(node2)
		tree.AddNode(node3)

		tree.AddNode(node11)
		tree.AddNode(node21)
		tree.SetRoot(node21)
		tree.AddNode(node31)
		tree.AddNode(node41)

		err := tree.IsCyclic()
		assert.Equal(t, job.ErrCyclicDependencyEncountered, err.Error())
	})

}
