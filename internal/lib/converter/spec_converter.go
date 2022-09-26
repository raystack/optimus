package converter

import (
	"github.com/odpf/optimus/client/local"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type SpecConverter[S local.JobSpec | local.ResourceSpec, P pb.JobSpecification | pb.ResourceSpecification] interface {
	ToProto(*S) P
	ToSpec(*P) S
}

type specConverter[S local.JobSpec | local.ResourceSpec, P pb.JobSpecification | pb.ResourceSpecification] struct {
	toProto func(*S) P
	toSpec  func(*P) S
}

func newConverter[S local.JobSpec | local.ResourceSpec, P pb.JobSpecification | pb.ResourceSpecification](toProto func(*S) P, toSpec func(*P) S) SpecConverter[S, P] {
	return &specConverter[S, P]{
		toProto: toProto,
		toSpec:  toSpec,
	}
}

func (c *specConverter[S, P]) ToProto(s *S) P {
	return c.toProto(s)
}

func (c *specConverter[S, P]) ToSpec(p *P) S {
	return c.toSpec(p)
}
