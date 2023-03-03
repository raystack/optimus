package errors_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/errors"
)

func TestErrors(t *testing.T) {
	t.Run("ErrorType", func(t *testing.T) {
		t.Run("returns string representation of an error", func(t *testing.T) {
			errStr := errors.ErrNotFound.String()
			assert.Equal(t, "not found", errStr)
		})
	})
	t.Run("DomainError", func(t *testing.T) {
		testEntity := "test"

		t.Run("creates error for not found", func(t *testing.T) {
			notFound := errors.NotFound(testEntity, "object not found")

			assert.Error(t, notFound)
			assert.ErrorContains(t, notFound, "object not found")
		})
		t.Run("creates error for already exists", func(t *testing.T) {
			alreadyExists := errors.AlreadyExists(testEntity, "resource is already there")

			assert.Error(t, alreadyExists)
			assert.ErrorContains(t, alreadyExists, "resource is already there")
		})
		t.Run("creates error for internal error", func(t *testing.T) {
			wrappedErr := fmt.Errorf("some external error")
			internalError := errors.InternalError(testEntity, "object has error", wrappedErr)

			assert.Error(t, internalError)
			assert.ErrorContains(t, internalError, "object has error")
		})
		t.Run("creates error for invalid argument", func(t *testing.T) {
			invalidArgument := errors.InvalidArgument(testEntity, "argument is not valid")

			assert.Error(t, invalidArgument)
			assert.ErrorContains(t, invalidArgument, "argument is not valid")
		})
		t.Run("creates error for invalid state transition", func(t *testing.T) {
			invalidStateTransition := errors.InvalidStateTransition(testEntity, "transition is invalid")

			assert.Error(t, invalidStateTransition)
			assert.ErrorContains(t, invalidStateTransition, "transition is invalid")
		})
		t.Run("creates a domain error", func(t *testing.T) {
			domainError := errors.NewError(errors.ErrFailedPrecond, testEntity, "random error")

			assert.Error(t, domainError)
			assert.ErrorContains(t, domainError, "random error")
			assert.True(t, errors.IsErrorType(domainError, errors.ErrFailedPrecond))
			assert.False(t, errors.IsErrorType(domainError, errors.ErrInternalError))
		})
		t.Run("returns true if error is type", func(t *testing.T) {
			notFound := errors.NotFound(testEntity, "object not found")

			isType := errors.Is(notFound, &errors.DomainError{})
			assert.True(t, isType)
		})
		t.Run("returns error as type with as", func(t *testing.T) {
			notFound := errors.NotFound(testEntity, "object not found")

			var de *errors.DomainError
			isType := errors.As(notFound, &de)
			assert.True(t, isType)
			assert.NotNil(t, de)
			assert.Equal(t, errors.ErrNotFound, de.ErrorType)
		})
		t.Run("adds context to error", func(t *testing.T) {
			notFound := errors.NotFound(testEntity, "object not found")

			errWithContext := errors.AddErrContext(notFound, testEntity, "error during add")

			assert.Error(t, errWithContext)
			assert.ErrorContains(t, errWithContext, "not found for entity test: error during add")
		})
		t.Run("wraps the error", func(t *testing.T) {
			invalidArg := errors.InvalidArgument(testEntity, "invalid arg")

			wrappedErr := errors.Wrap(testEntity, "wrapping error", invalidArg)
			assert.ErrorContains(t, wrappedErr, "wrapping error")
		})
		t.Run("returns the error debug string", func(t *testing.T) {
			notFound := errors.NotFound(testEntity, "object not found")

			errWithContext := errors.AddErrContext(notFound, testEntity, "error during add")

			str := errWithContext.DebugString()
			assert.Equal(t, "not found for test: error during add (not found for test: object not found ())", str)
		})
		t.Run("returns the error debug string with other error", func(t *testing.T) {
			connError := fmt.Errorf("not able to connect to database")
			errWithContext := errors.AddErrContext(connError, testEntity, "error during calling db")

			str := errWithContext.DebugString()
			assert.Equal(t, "internal error for test: error during calling db (not able to connect to database)", str)
		})
		t.Run("converts to grpc error", func(t *testing.T) {
			errTypesToTest := []errors.ErrorType{
				errors.ErrInternalError,
				errors.ErrNotFound,
				errors.ErrAlreadyExists,
				errors.ErrInvalidArgument,
				errors.ErrFailedPrecond,
			}

			for _, errorType := range errTypesToTest {
				err := errors.NewError(errorType, testEntity, "testing grpc error")
				grpcErr := errors.GRPCErr(err, "to grpc err")

				assert.ErrorContains(t, grpcErr, errorType.String())
				assert.ErrorContains(t, grpcErr, "testing grpc error")
			}
		})
	})
}
