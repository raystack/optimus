package writer

import (
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type DeployJobSpecificationResponseWriter interface {
	LogWriter
	SendDeploymentID(string) error
}

type deployJobSpecificationResponseWriter struct {
	stream pb.JobSpecificationService_DeployJobSpecificationServer
}

func NewDeployJobSpecificationResponseWriter(stream pb.JobSpecificationService_DeployJobSpecificationServer) DeployJobSpecificationResponseWriter {
	return &deployJobSpecificationResponseWriter{
		stream: stream,
	}
}

func (s *deployJobSpecificationResponseWriter) Write(level LogLevel, message string) error {
	logStatus := newLogStatusProto(level, message)
	resp := pb.DeployJobSpecificationResponse{
		LogStatus: logStatus,
	}
	return s.stream.Send(&resp)
}

func (s *deployJobSpecificationResponseWriter) SendDeploymentID(deployID string) error {
	resp := pb.DeployJobSpecificationResponse{
		DeploymentId: deployID,
	}
	return s.stream.Send(&resp)
}
