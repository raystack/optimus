package logger

import (
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/salt/log"
)

func PrintLogStatus(logger log.Logger, logStatus *pb.Log) {
	switch logStatus.GetLevel() {
	case pb.Level_LEVEL_INFO:
		logger.Info(logStatus.GetMessage())
	case pb.Level_LEVEL_WARNING:
		logger.Warn(ColoredNotice(logStatus.GetMessage()))
	case pb.Level_LEVEL_ERROR:
		logger.Error(ColoredError(logStatus.GetMessage()))
	default:
		logger.Debug(logStatus.GetMessage())
	}
}
