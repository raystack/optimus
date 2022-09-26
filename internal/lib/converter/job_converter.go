package converter

import (
	"github.com/odpf/optimus/client/local"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

func NewJobSpecConverter() SpecConverter[local.JobSpec, pb.JobSpecification] {
	return newConverter(jobSpecToProto, jobProtoToSpec)
}

func jobSpecToProto(s *local.JobSpec) pb.JobSpecification {
	// TODO: implement conversion here
	return pb.JobSpecification{}
}

func jobProtoToSpec(p *pb.JobSpecification) local.JobSpec {
	// TODO: implement conversion here
	return local.JobSpec{}
}
