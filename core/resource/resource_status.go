package resource

import (
	"fmt"

	"github.com/odpf/optimus/internal/errors"
)

func (r *Resource) MarkValidationSuccess() error {
	if r.status == StatusUnknown {
		r.status = StatusValidationSuccess
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status [%s] is not allowed", r.FullName(), r.status, StatusValidationSuccess)
	return errors.InvalidStateTransition(EntityResource, msg)
}

func (r *Resource) MarkValidationFailure() error {
	if r.status == StatusUnknown {
		r.status = StatusValidationFailure
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status [%s] is not allowed", r.FullName(), r.status, StatusValidationFailure)
	return errors.InvalidStateTransition(EntityResource, msg)
}

func (r *Resource) MarkSkipped() error {
	if r.status == StatusValidationSuccess {
		r.status = StatusSkipped
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status [%s] is not allowed", r.FullName(), r.status, StatusSkipped)
	return errors.InvalidStateTransition(EntityResource, msg)
}

func (r *Resource) MarkToCreate() error {
	if r.status == StatusValidationSuccess {
		r.status = StatusToCreate
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status [%s] is not allowed", r.FullName(), r.status, StatusToCreate)
	return errors.InvalidStateTransition(EntityResource, msg)
}

func (r *Resource) MarkToUpdate() error {
	if r.status == StatusValidationSuccess {
		r.status = StatusToUpdate
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status [%s] is not allowed", r.FullName(), r.status, StatusToUpdate)
	return errors.InvalidStateTransition(EntityResource, msg)
}

func (r *Resource) MarkExistInStore() error {
	if r.status == StatusToCreate {
		r.status = StatusExistInStore
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status [%s] is not allowed", r.FullName(), r.status, StatusExistInStore)
	return errors.InvalidStateTransition(EntityResource, msg)
}

func (r *Resource) MarkFailure() error {
	switch r.status {
	case StatusToCreate:
		r.status = StatusCreateFailure
		return nil
	case StatusToUpdate:
		r.status = StatusUpdateFailure
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status failure is not allowed", r.FullName(), r.status)
	return errors.InvalidStateTransition(EntityResource, msg)
}

func (r *Resource) MarkSuccess() error {
	switch r.status {
	case StatusToCreate, StatusToUpdate:
		r.status = StatusSuccess
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status success is not allowed", r.FullName(), r.status)
	return errors.InvalidStateTransition(EntityResource, msg)
}
