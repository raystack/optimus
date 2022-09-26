package converter

import (
	"github.com/odpf/optimus/client/local"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

func NewResourceSpecConverter() SpecConverter[local.ResourceSpec, pb.ResourceSpecification] {
	return newConverter(resourceSpecToProto, resourceProtoToSpec)
}

func resourceSpecToProto(s *local.ResourceSpec) pb.ResourceSpecification {
	// TODO: implement conversion here
	return pb.ResourceSpecification{}
}

func resourceProtoToSpec(p *pb.ResourceSpecification) local.ResourceSpec {
	// TODO: implement conversion here
	return local.ResourceSpec{}
}
