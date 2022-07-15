package sender

import (
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
)

type LogStatus interface {
	Send(pb.Log) error
}

func SendErrorMessage(logSender LogStatus, msg string) {
	sendMsg(logSender, pb.Level_Error, msg)
}

func SendWarningMessage(logSender LogStatus, msg string) {
	sendMsg(logSender, pb.Level_Warning, msg)
}

func SendSuccessMessage(logSender LogStatus, msg string) {
	sendMsg(logSender, pb.Level_Info, msg)
}

func sendMsg(logSender LogStatus, level pb.Level, msg string) {
	logSender.Send(pb.Log{
		Level:   level,
		Message: msg,
	})
}
