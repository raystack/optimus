package dag_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/odpf/optimus/core/dag"

	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestMultiRootDagTree(t *testing.T) {
	t.Run("String()AndDependents", func(t *testing.T) {
		treeNode1 := dag.NewTreeNode(models.JobSpec{
			Name: "job1",
		})
		treeNode2 := dag.NewTreeNode(models.JobSpec{
			Name: "job2",
		})
		multiRootTree := dag.NewMultiRootDAG()
		treeNode1.AddEdge(treeNode2)
		treeNode2.AddEdge(treeNode1)
		multiRootTree.AddNodeIfNotExist(treeNode1)
		multiRootTree.AddNodeIfNotExist(treeNode2)

		err := multiRootTree.IsCyclic()
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), dag.ErrCyclicDependencyEncountered.Error())
	})
	t.Run("GetRootNodes", func(t *testing.T) {
		treeNode1 := dag.NewTreeNode(models.JobSpec{
			Name: "job1",
		})
		multiRootTree := dag.NewMultiRootDAG()
		multiRootTree.AddNode(treeNode1)
		rootNodes := multiRootTree.GetRootNodes()
		assert.Equal(t, 1, len(rootNodes))
		assert.Equal(t, "job1", rootNodes[0].String())
	})
	t.Run("IsCyclic", func(t *testing.T) {
		t.Run("should throw an error if cyclic", func(t *testing.T) {
			treeNode1 := dag.NewTreeNode(models.JobSpec{
				Name: "job1",
			})
			treeNode2 := dag.NewTreeNode(models.JobSpec{
				Name: "job2",
			})
			multiRootTree := dag.NewMultiRootDAG()
			multiRootTree.AddNode(treeNode1)
			multiRootTree.AddNode(treeNode2)
			treeNode1.AddEdge(treeNode2)
			treeNode2.AddEdge(treeNode1)
			err := multiRootTree.IsCyclic()
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "cycle dependency")
		})
		t.Run("should not return error if not cyclic", func(t *testing.T) {
			treeNode1 := dag.NewTreeNode(models.JobSpec{
				Name: "job1",
			})
			treeNode2 := dag.NewTreeNode(models.JobSpec{
				Name: "job2",
			})
			multiRootTree := dag.NewMultiRootDAG()
			multiRootTree.AddNode(treeNode1)
			multiRootTree.AddNode(treeNode2)
			treeNode1.AddEdge(treeNode2)
			err := multiRootTree.IsCyclic()
			assert.Nil(t, err)
		})
	})
	t.Run("DFS", func(t *testing.T) {
		t.Run("should correctly maintain the DFS order", func(t *testing.T) {
			expectedOrder1 := "1,2,3,4,5,"
			expectedOrder2 := "1,3,4,5,2,"

			g := dag.NewMultiRootDAG()
			n1 := dag.BasicNode(models.JobSpec{Name: "1"})
			n2 := dag.BasicNode(models.JobSpec{Name: "2"})
			n3 := dag.BasicNode(models.JobSpec{Name: "3"})
			n4 := dag.BasicNode(models.JobSpec{Name: "4"})
			n5 := dag.BasicNode(models.JobSpec{Name: "5"})
			g.Connect(n1, n2)
			g.Connect(n1, n3)
			g.Connect(n3, n4)
			g.Connect(n4, n5)

			finalOrder := ""
			g.DFS(n1, func(v *dag.TreeNode, d int) error {
				finalOrder += v.String() + ","
				return nil
			})
			if finalOrder != expectedOrder1 && finalOrder != expectedOrder2 {
				assert.Error(t, fmt.Errorf("incorrect DFS order, expected either %s or %s, got %s", expectedOrder1, expectedOrder2, finalOrder))
			}
		})
	})
	t.Run("ReverseTopologicalSort", func(t *testing.T) {
		t.Run("should correctly maintain the ReverseTopologicalSort order", func(t *testing.T) {
			g := dag.NewMultiRootDAG()
			n1 := dag.BasicNode(models.JobSpec{Name: "1"})
			n2 := dag.BasicNode(models.JobSpec{Name: "2"})
			n3 := dag.BasicNode(models.JobSpec{Name: "3"})
			n4 := dag.BasicNode(models.JobSpec{Name: "4"})
			n5 := dag.BasicNode(models.JobSpec{Name: "5"})
			g.Connect(n1, n2)
			g.Connect(n1, n3)
			g.Connect(n3, n4)
			g.Connect(n4, n5)

			expectedOrder1 := []*dag.TreeNode{n2, n5, n4, n3, n1}
			expectedOrder2 := []*dag.TreeNode{n5, n4, n3, n1, n2}

			finalOrder := g.ReverseTopologicalSort(n1)
			if !reflect.DeepEqual(finalOrder, expectedOrder1) && !reflect.DeepEqual(finalOrder, expectedOrder2) {
				assert.Error(t, fmt.Errorf("incorrect DFS order, expected either %s or %s, got %s", expectedOrder1, expectedOrder2, finalOrder))
			}
		})
	})
	t.Run("TransitiveReduction", func(t *testing.T) {
		t.Run("should correctly reduce the graph", func(t *testing.T) {
			g := dag.NewMultiRootDAG()
			n1 := dag.BasicNode(models.JobSpec{Name: "1"})
			n2 := dag.BasicNode(models.JobSpec{Name: "2"})
			n3 := dag.BasicNode(models.JobSpec{Name: "3"})
			n4 := dag.BasicNode(models.JobSpec{Name: "4"})
			n5 := dag.BasicNode(models.JobSpec{Name: "5"})
			g.Connect(n1, n2)
			g.Connect(n1, n3)
			g.Connect(n1, n5)
			g.Connect(n3, n4)
			g.Connect(n4, n5)
			g.Connect(n3, n5)
			g.TransitiveReduction()

			finalNode1, ok := g.GetNodeByName("1")
			assert.Equal(t, ok, true)
			for _, edge := range finalNode1.Edges {
				if edge.String() == n5.String() {
					assert.Error(t, fmt.Errorf("%s edge is unexpected after reduction", n5.String()))
				}
			}

			finalNode3, ok := g.GetNodeByName("3")
			assert.Equal(t, ok, true)
			for _, edge := range finalNode3.Edges {
				if edge.String() == n5.String() {
					assert.Error(t, fmt.Errorf("%s edge is unexpected after reduction", n5.String()))
				}
			}
		})
	})
}
