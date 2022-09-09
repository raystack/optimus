package tree_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/models"
)

func TestMultiRootDagTree(t *testing.T) {
	t.Run("GetNameAndDependents", func(t *testing.T) {
		treeNode1 := tree.NewTreeNode(models.JobSpec{
			Name: "job1",
		})
		treeNode2 := tree.NewTreeNode(models.JobSpec{
			Name: "job2",
		})
		multiRootTree := tree.NewMultiRootTree()
		treeNode1.AddDependent(treeNode2)
		treeNode2.AddDependent(treeNode1)
		multiRootTree.AddNodeIfNotExist(treeNode1)
		multiRootTree.AddNodeIfNotExist(treeNode2)

		err := multiRootTree.IsCyclic()
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), tree.ErrCyclicDependencyEncountered.Error())
	})
	t.Run("MarkRoot", func(t *testing.T) {
		treeNode1 := tree.NewTreeNode(models.JobSpec{
			Name: "job1",
		})
		multiRootTree := tree.NewMultiRootTree()
		multiRootTree.AddNode(treeNode1)
		multiRootTree.MarkRoot(treeNode1)
		rootNodes := multiRootTree.GetRootNodes()
		assert.Equal(t, 1, len(rootNodes))
		assert.Equal(t, "job1", rootNodes[0].Data.GetName())
	})
	t.Run("IsCyclic", func(t *testing.T) {
		t.Run("should throw an error if cyclic", func(t *testing.T) {
			treeNode1 := tree.NewTreeNode(models.JobSpec{
				Name: "pilotdata-integration.playground.job1",
			})
			treeNode2 := tree.NewTreeNode(models.JobSpec{
				Name: "pilotdata-integration.playground.job2",
			})
			treeNode3 := tree.NewTreeNode(models.JobSpec{
				Name: "pilotdata-integration.playground.job3",
			})
			treeNode4 := tree.NewTreeNode(models.JobSpec{
				Name: "pilotdata-integration.playground.job4",
			})
			multiRootTree := tree.NewMultiRootTree()
			multiRootTree.AddNode(treeNode1)
			multiRootTree.AddNode(treeNode2)
			multiRootTree.AddNode(treeNode3)
			multiRootTree.AddNode(treeNode4)
			treeNode4.AddDependent(treeNode3)
			treeNode3.AddDependent(treeNode2)
			treeNode2.AddDependent(treeNode1)
			treeNode2.AddDependent(treeNode4)
			err := multiRootTree.IsCyclic()
			assert.NotNil(t, err)
			assert.Equal(t, `a cycle dependency encountered in the tree: 
->pilotdata-integration.playground.job2
	->pilotdata-integration.playground.job3
		->pilotdata-integration.playground.job4
			->pilotdata-integration.playground.job2`, err.Error())
		})
		t.Run("should not return error if not cyclic", func(t *testing.T) {
			treeNode1 := tree.NewTreeNode(models.JobSpec{
				Name: "job1",
			})
			treeNode2 := tree.NewTreeNode(models.JobSpec{
				Name: "job2",
			})
			multiRootTree := tree.NewMultiRootTree()
			multiRootTree.AddNode(treeNode1)
			multiRootTree.AddNode(treeNode2)
			treeNode1.AddDependent(treeNode2)
			err := multiRootTree.IsCyclic()
			assert.Nil(t, err)
		})
	})
}
