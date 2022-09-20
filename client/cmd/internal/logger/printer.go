package logger

import (
	"github.com/odpf/salt/log"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
)

func PrintLogStatus(logger log.Logger, logStatus *pb.Log) {
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
