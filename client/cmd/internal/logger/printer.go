package logger

import (
	"github.com/raystack/salt/log"

	pb "github.com/raystack/optimus/protos/raystack/optimus/core/v1beta1"
)

func PrintLogStatusVerbose(logger log.Logger, logStatus *pb.Log) {
	switch logStatus.GetLevel() {
	case pb.Level_LEVEL_INFO:
		logger.Info(logStatus.GetMessage())
	case pb.Level_LEVEL_WARNING:
		logger.Warn(logStatus.GetMessage())
	case pb.Level_LEVEL_ERROR:
		logger.Error(logStatus.GetMessage())
	default:
		logger.Debug(logStatus.GetMessage())
	}
}

func PrintLogStatus(logger log.Logger, logStatus *pb.Log) {
	if logStatus.GetLevel() == pb.Level_LEVEL_ERROR {
		logger.Error(logStatus.GetMessage())
	}
}
