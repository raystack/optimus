package writer

import (
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
)

type RefreshJobResponseWriter interface {
	LogWriter
	SendDeploymentID(string) error
}

type refreshJobResponseWriter struct {
	stream pb.JobSpecificationService_RefreshJobsServer
}

func NewRefreshJobResponseWriter(stream pb.JobSpecificationService_RefreshJobsServer) RefreshJobResponseWriter {
	return &refreshJobResponseWriter{
		stream: stream,
	}
}

func (s *refreshJobResponseWriter) Write(level LogLevel, message string) error {
	logStatus := newLogStatusProto(level, message)
	resp := pb.RefreshJobsResponse{
		LogStatus: logStatus,
	}
	return s.stream.Send(&resp)
}

func (s *refreshJobResponseWriter) SendDeploymentID(deployID string) error {
	resp := pb.RefreshJobsResponse{
		DeploymentId: deployID,
	}
	return s.stream.Send(&resp)
}
