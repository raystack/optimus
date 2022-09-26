package converter

import (
	"github.com/odpf/optimus/client/local"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type Converter[S local.JobSpec | local.ResourceSpec, P pb.JobSpecification | pb.ResourceSpecification] interface {
	ToProto(*S) P
	ToSpec(*P) S
}

type converter[S local.JobSpec | local.ResourceSpec, P pb.JobSpecification | pb.ResourceSpecification] struct {
	toProto func(*S) P
	toSpec  func(*P) S
}

func newConverter[S local.JobSpec | local.ResourceSpec, P pb.JobSpecification | pb.ResourceSpecification](toProto func(*S) P, toSpec func(*P) S) Converter[S, P] {
	return &converter[S, P]{
		toProto: toProto,
		toSpec:  toSpec,
	}
}

func (c *converter[S, P]) ToProto(s *S) P {
	return c.toProto(s)
}

func (c *converter[S, P]) ToSpec(p *P) S {
	return c.toSpec(p)
}
