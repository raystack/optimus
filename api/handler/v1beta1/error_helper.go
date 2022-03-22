package v1beta1

import (
	"errors"

	"github.com/odpf/optimus/service"
	"github.com/odpf/salt/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mapToGRPCErr(l log.Logger, err error, msg string) error {
	code := codes.Internal
	var de *service.DomainError
	if errors.As(err, &de) {
		switch de.ErrorType {
		case service.ErrNotFound:
			code = codes.NotFound
		case service.ErrInvalidArgument:
			code = codes.InvalidArgument
		case service.ErrAlreadyExists:
			code = codes.AlreadyExists
		case service.ErrFailedPrecond:
			code = codes.FailedPrecondition
		}

		l.Error(de.DebugString())
	}

	return status.Errorf(code, "%s: %s", err.Error(), msg)
}
