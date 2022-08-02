package logger

import (
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/salt/log"
)

func PrintLogStatus(logger log.Logger, logStatus *pb.Log) {
	switch logStatus.GetLevel() {
	case pb.Level_Info:
		logger.Info(logStatus.GetMessage())
	case pb.Level_Warning:
		logger.Warn(logStatus.GetMessage())
	case pb.Level_Error:
		logger.Error(logStatus.GetMessage())
	}
}
