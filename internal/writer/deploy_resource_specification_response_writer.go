package writer

import (
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type deployResourceSpecificationResponseWriter struct {
	stream pb.ResourceService_DeployResourceSpecificationServer
}

func NewDeployResourceSpecificationResponseWriter(stream pb.ResourceService_DeployResourceSpecificationServer) LogWriter {
	return &deployResourceSpecificationResponseWriter{stream: stream}
}

func (l *deployResourceSpecificationResponseWriter) Write(level LogLevel, message string) error {
	logStatus := newLogStatusProto(level, message)
	resp := pb.DeployResourceSpecificationResponse{
		LogStatus: logStatus,
	}
	return l.stream.Send(&resp)
}
