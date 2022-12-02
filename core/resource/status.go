package resource

import "strings"

type Status string

const (
	StatusUnknown           Status = "unknown"
	StatusValidationFailure Status = "validation_failure"
	StatusValidationSuccess Status = "validation_success"
	StatusToCreate          Status = "to_create"
	StatusToUpdate          Status = "to_update"
	StatusSkipped           Status = "skipped"
	StatusCreateFailure     Status = "create_failure"
	StatusUpdateFailure     Status = "update_failure"
	StatusExistInStore      Status = "exist_in_store"
	StatusSuccess           Status = "success"
)

func (s Status) String() string {
	return string(s)
}

func FromStringToStatus(status string) Status {
	switch strings.ToLower(status) {
	case StatusValidationFailure.String():
		return StatusValidationFailure
	case StatusValidationSuccess.String():
		return StatusValidationSuccess
	case StatusToCreate.String():
		return StatusToCreate
	case StatusToUpdate.String():
		return StatusToUpdate
	case StatusSkipped.String():
		return StatusSkipped
	case StatusCreateFailure.String():
		return StatusCreateFailure
	case StatusUpdateFailure.String():
		return StatusUpdateFailure
	case StatusExistInStore.String():
		return StatusExistInStore
	case StatusSuccess.String():
		return StatusSuccess
	default:
		return StatusUnknown
	}
}

func StatusForToCreate(status Status) bool {
	return status == StatusCreateFailure || status == StatusToCreate
}

func StatusForToUpdate(status Status) bool {
	return status == StatusSuccess ||
		status == StatusToUpdate ||
		status == StatusExistInStore ||
		status == StatusUpdateFailure
}

func StatusIsSuccess(status Status) bool {
	return status == StatusSuccess
}
