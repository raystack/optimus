package writer

import (
	pb "github.com/raystack/optimus/protos/raystack/optimus/core/v1beta1"
)

type checkJobSpecificationResponseWriter struct {
	stream pb.JobSpecificationService_CheckJobSpecificationsServer
}

func NewCheckJobSpecificationResponseWriter(stream pb.JobSpecificationService_CheckJobSpecificationsServer) LogWriter {
	return &checkJobSpecificationResponseWriter{
		stream: stream,
	}
}

func (s *checkJobSpecificationResponseWriter) Write(level LogLevel, message string) error {
	logStatus := newLogStatusProto(level, message)
	resp := pb.CheckJobSpecificationsResponse{
		LogStatus: logStatus,
	}
	return s.stream.Send(&resp)
}
