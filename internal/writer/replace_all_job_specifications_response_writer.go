package writer

import (
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type ReplaceAllJobSpecificationsResponseWriter interface {
	LogWriter
}

type replaceAllJobSpecificationsResponseWriter struct {
	stream pb.JobSpecificationService_ReplaceAllJobSpecificationsServer
}

func NewReplaceAllJobSpecificationsResponseWriter(stream pb.JobSpecificationService_ReplaceAllJobSpecificationsServer) ReplaceAllJobSpecificationsResponseWriter {
	return &replaceAllJobSpecificationsResponseWriter{
		stream: stream,
	}
}

func (s *replaceAllJobSpecificationsResponseWriter) Write(level LogLevel, message string) error {
	logStatus := newLogStatusProto(level, message)
	resp := pb.ReplaceAllJobSpecificationsResponse{
		LogStatus: logStatus,
	}
	return s.stream.Send(&resp)
}
