package sender

import (
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
)

type refreshJobLogStatus struct {
	stream pb.JobSpecificationService_RefreshJobsServer
}

func NewRefreshJobLogStatus(stream pb.JobSpecificationService_RefreshJobsServer) LogStatus {
	return &refreshJobLogStatus{stream: stream}
}

func (l *refreshJobLogStatus) Send(logStatus pb.Log) error {
	resp := pb.RefreshJobsResponse{
		LogStatus: &logStatus,
	}
	return l.stream.Send(&resp)
}
