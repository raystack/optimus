package errors

import (
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
)

type ErrorType string

func (s ErrorType) String() string {
	return strings.ToLower(string(s))
}

const (
	ErrInternalError   ErrorType = "Internal Error"
	ErrNotFound        ErrorType = "Not Found"
	ErrAlreadyExists   ErrorType = "Resource Already Exists"
	ErrInvalidArgument ErrorType = "Invalid Argument"
	ErrFailedPrecond   ErrorType = "Failed Precondition"
)

type DomainError struct {
	ErrorType  ErrorType
	Entity     string
	Message    string
	WrappedErr error
}

func NewError(errType ErrorType, entity string, msg string) *DomainError {
	return &DomainError{
		Entity:     entity,
		ErrorType:  errType,
		Message:    msg,
		WrappedErr: nil,
	}
}

func NewInternalError(entity string, msg string, err error) *DomainError {
	return &DomainError{
		Entity:     entity,
		ErrorType:  ErrInternalError,
		Message:    msg,
		WrappedErr: err,
	}
}

func NewInvalidArgumentError(entity string, msg string) *DomainError {
	return &DomainError{
		ErrorType:  ErrInvalidArgument,
		Entity:     entity,
		Message:    msg,
		WrappedErr: nil,
	}
}

func NewNotFoundError(entity string, msg string) *DomainError {
	return &DomainError{
		ErrorType:  ErrNotFound,
		Entity:     entity,
		Message:    msg,
		WrappedErr: nil,
	}
}

func (e *DomainError) Error() string {
	return fmt.Sprintf("%v for entity %v: %v",
		e.ErrorType.String(), e.Entity, e.Message)
}

func (e *DomainError) Unwrap() error {
	return e.WrappedErr
}

func MapToGRPCErr(err error, msg string) error {
	code := codes.Internal
	var de *DomainError
	if errors.As(err, &de) {
		switch de.ErrorType {
		case ErrNotFound:
			code = codes.NotFound
		case ErrInvalidArgument:
			code = codes.InvalidArgument
		case ErrAlreadyExists:
			code = codes.AlreadyExists
		case ErrFailedPrecond:
			code = codes.FailedPrecondition
		}
	}
	return status.Errorf(code, "%s: %s", err.Error(), msg)
}
