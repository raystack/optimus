package writer

import (
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

type RefreshJobResponseWriter interface {
	LogWriter
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
