package dag

// Edge represents an edge in the graph, with a source and target
type Edge interface {
	Source() *TreeNode
	Target() *TreeNode

	Hashable
}

// BasicEdge returns an Edge implementation
func BasicEdge(source, target *TreeNode) Edge {
	return &basicEdge{S: source, T: target}
}

// basicEdge is a basic implementation of Edge that has the source and
// target vertex
type basicEdge struct {
	S, T *TreeNode
}

func (e *basicEdge) Hashcode() interface{} {
	return [...]interface{}{1, 2}
}

func (e *basicEdge) Source() *TreeNode {
	return e.S
}

func (e *basicEdge) Target() *TreeNode {
	return e.T
}

// Hashable is the interface used by set to get the hash code of a value.
type Hashable interface {
	Hashcode() interface{}
}

// hashcode returns the hashcode used for element, if not, default value
func hashcode(v interface{}) interface{} {
	if h, ok := v.(Hashable); ok {
		return h.Hashcode()
	}
	return v
}
