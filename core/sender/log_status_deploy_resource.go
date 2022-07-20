package sender

import (
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
)

type deployResourceLogStatus struct {
	stream pb.ResourceService_DeployResourceSpecificationServer
}

func NewDeployResourceLogStatus(stream pb.ResourceService_DeployResourceSpecificationServer) LogStatus {
	return &deployResourceLogStatus{stream: stream}
}

func (l *deployResourceLogStatus) Send(logStatus *pb.Log) error {
	resp := pb.DeployResourceSpecificationResponse{
		LogStatus: logStatus,
	}
	return l.stream.Send(&resp)
}
