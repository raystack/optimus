package tree_test

import (
	"strings"
	"testing"

	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestMultiRootDagTree(t *testing.T) {
	t.Run("GetNameAndDependents", func(t *testing.T) {
		multiRootTree := tree.NewMultiRootTree()
		treeNode1 := tree.NewTreeNode(models.JobSpec{
			Name: "job1",
		})
		treeNode2 := tree.NewTreeNode(models.JobSpec{
			Name: "job2",
		})
		treeNode1.AddDependent(treeNode2)
		treeNode2.AddDependent(treeNode1)
		multiRootTree.AddNodeIfNotExist(treeNode1)
		multiRootTree.AddNodeIfNotExist(treeNode2)

		err := multiRootTree.IsCyclic()
		assert.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), tree.ErrCyclicDependencyEncountered.Error()))
	})
}
