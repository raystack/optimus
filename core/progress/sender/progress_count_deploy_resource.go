package sender

import (
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
)

type deployResourceProgressCount struct {
	stream pb.ResourceService_DeployResourceSpecificationServer
}

func NewDeployResourceProgressCount(stream pb.ResourceService_DeployResourceSpecificationServer) ProgressCount {
	return &deployResourceProgressCount{stream: stream}
}

func (l *deployResourceProgressCount) Add(count int) error {
	resp := pb.DeployResourceSpecificationResponse{
		Progress: &pb.Progress{Count: int32(count)},
	}
	return l.stream.Send(&resp)
}

func (l *deployResourceProgressCount) Inc() error {
	return l.Add(1)
}
