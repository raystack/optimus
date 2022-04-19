package dag

import "github.com/pkg/errors"

var (
	// ErrCyclicDependencyEncountered is triggered a tree has a cyclic dependency
	ErrCyclicDependencyEncountered = errors.New("a cycle dependency encountered in the tree")
)

// MultiRootDAG - represents a data type which has multiple independent root nodes
// all root nodes have their independent tree based on dependencies of TreeNode.
// It also maintains a map of nodes for faster lookups and managing node data.
type MultiRootDAG struct {
	dataMap map[string]*TreeNode
}

// GetRootNodes return root nodes of graph
// O(VE)
func (t *MultiRootDAG) GetRootNodes() []*TreeNode {
	candidates := make(map[*TreeNode]struct{})
	// mark all as root node
	for _, node := range t.dataMap {
		candidates[node] = struct{}{}
	}

	for _, node := range t.dataMap {
		for _, edge := range node.Edges {
			if edge.String() == node.String() {
				delete(candidates, node)
			}
		}
	}

	var roots []*TreeNode
	for candidate, _ := range candidates {
		roots = append(roots, candidate)
	}
	return roots
}

// AddNode adds a new node or overwrite an existing one
// in graph
func (t *MultiRootDAG) AddNode(node *TreeNode) {
	t.dataMap[node.String()] = node
}

func (t *MultiRootDAG) AddNodeIfNotExist(node *TreeNode) {
	_, ok := t.GetNodeByName(node.String())
	if !ok {
		t.AddNode(node)
	}
}

func (t *MultiRootDAG) GetNodeByName(dagName string) (*TreeNode, bool) {
	value, ok := t.dataMap[dagName]
	return value, ok
}

func (t *MultiRootDAG) Nodes() []*TreeNode {
	var nodes []*TreeNode
	for _, node := range t.dataMap {
		nodes = append(nodes, node)
	}
	return nodes
}

func (t *MultiRootDAG) Edges() []Edge {
	var edges []Edge
	for _, node := range t.dataMap {
		for _, adjacentNode := range node.Edges {
			edges = append(edges, BasicEdge(node, adjacentNode))
		}
	}
	return edges
}

// Connect adds source and target nodes if not present already and
// mark target node as directed edge from source
func (t *MultiRootDAG) Connect(source *TreeNode, target *TreeNode) {
	t.AddNodeIfNotExist(source)
	t.AddNodeIfNotExist(target)
	// check if already an edge
	exists := false
	for _, edge := range t.dataMap[source.String()].Edges {
		if edge.String() == target.String() {
			exists = true
		}
	}
	if !exists {
		t.dataMap[source.String()].AddEdge(target)
	}
}

// Disconnect removes edge from source to target
func (t *MultiRootDAG) Disconnect(source *TreeNode, target *TreeNode) {
	_, ok := t.dataMap[source.String()]
	if !ok {
		return
	}
	t.dataMap[source.String()].RemoveEdge(target)
}

// IsCyclic - detects if there are any cycles in the tree
func (t *MultiRootDAG) IsCyclic() error {
	visitedMap := make(map[string]bool)
	for _, node := range t.dataMap {
		if _, visited := visitedMap[node.String()]; !visited {
			pathMap := make(map[string]bool)
			err := t.hasCycle(node, visitedMap, pathMap)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// runs a DFS on a given tree using visitor pattern
func (t *MultiRootDAG) hasCycle(root *TreeNode, visited, pathMap map[string]bool) error {
	_, isNodeVisited := visited[root.String()]
	if !isNodeVisited || !visited[root.String()] {
		pathMap[root.String()] = true
		visited[root.String()] = true
		var cyclicErr error
		for _, child := range root.Edges {
			n, _ := t.GetNodeByName(child.String())
			_, isChildVisited := visited[child.String()]
			if !isChildVisited || !visited[child.String()] {
				cyclicErr = t.hasCycle(n, visited, pathMap)
			}
			if cyclicErr != nil {
				return cyclicErr
			}

			_, childAlreadyInPath := pathMap[child.String()] // 1 -> 2 -> 1
			if childAlreadyInPath && pathMap[child.String()] {
				cyclicErr = errors.Wrap(ErrCyclicDependencyEncountered, root.String())
			}
			if cyclicErr != nil {
				return cyclicErr
			}
		}
		pathMap[root.String()] = false
	}
	return nil
}

// TransitiveReduction of a directed graph D is another directed graph
// with the same vertices and as few edges as possible, such that
// for all pairs of vertices v, w a (directed) path from v to w
// in D exists if and only if such a path exists in the reduction.
// The closely related concept of a minimum equivalent graph is a
// subgraph of D that has the same reachability relation and as
// few edges as possible.
// For example, if there are three nodes A => B => C, and A connects
// to both B and C, and B connects to C, then the transitive reduction
// is same graph with only a single edge between A and B, and a single edge
// between B and C.
// O(V(VE))
func (t *MultiRootDAG) TransitiveReduction() {
	// do DFS for each node
	for _, u := range t.Nodes() {
		uNeighbours := u.Edges

		for _, uNeighbour := range uNeighbours {
			t.DFS(uNeighbour, func(v *TreeNode, d int) error {
				if uNeighbour.String() == v.String() {
					// don't run this for yourself
					return nil
				}

				// TODO(kushsharma): can be improved to do this in O(N)
				// find intersection between nodes directly reachable from u
				// and nodes directly reachable by v
				var commonNeighbours []*TreeNode
				for _, vNeighbour := range v.Edges {
					for _, node := range uNeighbours {
						if vNeighbour.String() == node.String() {
							commonNeighbours = append(commonNeighbours, vNeighbour)
						}
					}
				}
				// if there is extra edge to reach from u -> x and x -> y
				// then remove x -> y
				for _, vPrime := range commonNeighbours {
					t.Disconnect(u, vPrime)
				}
				return nil
			})
		}
	}
}

type vertexAtDepth struct {
	Node  *TreeNode
	Depth int
}

// DFS applies depth first search on each vertex taking a walk function that also
// receives the current depth of the walk as an argument.
// Walk function is called top down
func (t *MultiRootDAG) DFS(root *TreeNode, f func(v *TreeNode, d int) error) error {
	seen := make(map[string]struct{})
	var stack []*vertexAtDepth
	stack = append(stack, &vertexAtDepth{
		Node:  root,
		Depth: 0,
	})
	for len(stack) > 0 {
		// pop the vertex from the top of the stack
		n := len(stack)
		current := stack[n-1]
		stack = stack[:n-1]

		// if seen, ignore
		if _, ok := seen[current.Node.String()]; ok {
			continue
		}
		// mark seen
		seen[current.Node.String()] = struct{}{}

		// visit the current node
		if err := f(current.Node, current.Depth); err != nil {
			return err
		}

		for _, v := range current.Node.Edges {
			stack = append(stack, &vertexAtDepth{
				Node:  v,
				Depth: current.Depth + 1,
			})
		}
	}
	return nil
}

func (t *MultiRootDAG) ReverseTopologicalSort(root *TreeNode) []*TreeNode {
	var sorted []*TreeNode
	_ = t.rdfsRunner(&vertexAtDepth{
		Node:  root,
		Depth: 0,
	}, make(map[string]struct{}), func(v *TreeNode, d int) error {
		sorted = append(sorted, v)
		return nil
	})
	return sorted
}

// rdfsRunner applies depth first search on each vertex taking a walk function that also
// receives the current depth of the walk as an argument.
// Walk function is called bottom up
func (t *MultiRootDAG) rdfsRunner(current *vertexAtDepth, seen map[string]struct{}, f func(v *TreeNode, d int) error) error {

	// if seen, ignore
	if _, ok := seen[current.Node.String()]; ok {
		return nil
	}
	// mark seen
	seen[current.Node.String()] = struct{}{}

	// do for all neighbours
	for _, v := range current.Node.Edges {
		if err := t.rdfsRunner(&vertexAtDepth{
			Node:  v,
			Depth: current.Depth + 1,
		}, seen, f); err != nil {
			return err
		}
	}

	// visit the current node
	if err := f(current.Node, current.Depth); err != nil {
		return err
	}
	return nil
}

// NewMultiRootDAG returns an instance of multi root dag tree
func NewMultiRootDAG() *MultiRootDAG {
	return &MultiRootDAG{
		dataMap: map[string]*TreeNode{},
	}
}
