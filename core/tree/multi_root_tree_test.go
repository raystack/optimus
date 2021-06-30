package tree_test

import (
	"testing"

	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
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
				Name: "job1",
			})
			treeNode2 := tree.NewTreeNode(models.JobSpec{
				Name: "job2",
			})
			multiRootTree := tree.NewMultiRootTree()
			multiRootTree.AddNode(treeNode1)
			multiRootTree.AddNode(treeNode2)
			treeNode1.AddDependent(treeNode2)
			treeNode2.AddDependent(treeNode1)
			err := multiRootTree.IsCyclic()
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "cycle dependency")
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
