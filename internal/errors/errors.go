package errors

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	ErrInvalidState ErrorType = "Invalid State"
)

type DomainError struct {
	ErrorType  ErrorType
	Entity     string
	Message    string
	WrappedErr error
}

func (*DomainError) Is(tgt error) bool {
	_, ok := tgt.(*DomainError) // nolint
	return ok
}

func AddErrContext(err error, entity, msg string) *DomainError {
	errType := ErrInternalError
	var de *DomainError
	if errors.As(err, &de) {
		errType = de.ErrorType
	}

	return &DomainError{
		ErrorType:  errType,
		Entity:     entity,
		Message:    msg,
		WrappedErr: err,
	}
}

func IsErrorType(err error, errType ErrorType) bool {
	var de *DomainError
	if errors.As(err, &de) {
		if de.ErrorType == errType {
			return true
		}
	}
	return false
}

func NewError(errType ErrorType, entity, msg string) *DomainError {
	return &DomainError{
		Entity:     entity,
		ErrorType:  errType,
		Message:    msg,
		WrappedErr: nil,
	}
}

func InternalError(entity, msg string, err error) *DomainError {
	return &DomainError{
		Entity:     entity,
		ErrorType:  ErrInternalError,
		Message:    msg,
		WrappedErr: err,
	}
}

func InvalidStateTransition(entity, msg string) *DomainError {
	return &DomainError{
		ErrorType:  ErrInvalidState,
		Entity:     entity,
		Message:    msg,
		WrappedErr: nil,
	}
}

func InvalidArgument(entity, msg string) *DomainError {
	return &DomainError{
		ErrorType:  ErrInvalidArgument,
		Entity:     entity,
		Message:    msg,
		WrappedErr: nil,
	}
}

func AlreadyExists(entity, msg string) *DomainError {
	return &DomainError{
		ErrorType:  ErrAlreadyExists,
		Entity:     entity,
		Message:    msg,
		WrappedErr: nil,
	}
}

func NotFound(entity, msg string) *DomainError {
	return &DomainError{
		ErrorType:  ErrNotFound,
		Entity:     entity,
		Message:    msg,
		WrappedErr: nil,
	}
}

func Is(err, target error) bool {
	return errors.Is(err, target)
}

func As(err error, target any) bool {
	return errors.As(err, target)
}

func (e *DomainError) Error() string {
	if e.WrappedErr != nil {
		return fmt.Sprintf("%v for entity %v: %v: %s",
			e.ErrorType.String(), e.Entity, e.Message, e.WrappedErr.Error())
	}
	return fmt.Sprintf("%v for entity %v: %v",
		e.ErrorType.String(), e.Entity, e.Message)
}

func (e *DomainError) Unwrap() error {
	return e.WrappedErr
}

func (e *DomainError) DebugString() string {
	var msg string
	var de *DomainError
	if errors.As(e.WrappedErr, &de) {
		msg = de.DebugString()
	} else if e.WrappedErr != nil {
		msg = e.WrappedErr.Error()
	}

	return fmt.Sprintf("%v for %v: %v (%s)",
		e.ErrorType.String(), e.Entity, e.Message, msg)
}

func Wrap(entity, msg string, err error) error {
	return &DomainError{
		ErrorType:  ErrInternalError,
		Entity:     entity,
		Message:    msg,
		WrappedErr: err,
	}
}

func WrapIfErr(entity, msg string, err error) error {
	if err == nil {
		return nil
	}

	return Wrap(entity, msg, err)
}

func GRPCErr(err error, msg string) error {
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
