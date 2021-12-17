package models

import "github.com/google/uuid"

// NamespaceSpec represents a namespace which is an individual or a team with an unique name.
// A Project can have any number of namespaces (with unique names).
type NamespaceSpec struct {
	ID uuid.UUID

	Name string

	Config map[string]string

	// ProjectSpec is the project that this namespace belongs to
	ProjectSpec ProjectSpec
}

const AllNamespace = "*"
