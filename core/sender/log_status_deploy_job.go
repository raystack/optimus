package sender

import (
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
)

type deployJobLogStatus struct {
	stream pb.JobSpecificationService_DeployJobSpecificationServer
}

func NewDeployJobLogStatus(stream pb.JobSpecificationService_DeployJobSpecificationServer) LogStatus {
	return &deployJobLogStatus{stream: stream}
}

func (l *deployJobLogStatus) Send(logStatus *pb.Log) error {
	resp := pb.DeployJobSpecificationResponse{
		LogStatus: logStatus,
	}
	return l.stream.Send(&resp)
}
