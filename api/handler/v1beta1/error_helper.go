package v1beta1

import (
	"github.com/odpf/optimus/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mapToGRPCErr(err error, msg string) error {
	code := codes.Internal
	de, ok := err.(*service.DomainError)
	if ok {
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
	}

	// TODO: Log the full error before returning
	return status.Errorf(code, "%s: %s", err.Error(), msg)
}
