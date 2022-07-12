package sender

import (
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
)

type LogStatus interface {
	Send(pb.Log) error
}

func SendErrorMessage(logSender LogStatus, msg string) {
	logSender.Send(pb.Log{
		Level:   pb.Level_Error,
		Message: msg,
	})
}

func SendSuccessMessage(logSender LogStatus, msg string) {
	logSender.Send(pb.Log{
		Level:   pb.Level_Info,
		Message: msg,
	})
}
