package job

type Status string

const (
	StatusUnknown Status = "unknown"
	StatusSkipped Status = "skipped"

	StatusValidationFailure Status = "validation_failure"
	StatusValidationSuccess Status = "validation_success"

	StatusToCreate Status = "to_create"
	StatusToUpdate Status = "to_update"

	StatusCreateFailure Status = "create_failure"
	StatusUpdateFailure Status = "update_failure"

	StatusSuccess Status = "success"
)

func (s Status) String() string {
	return string(s)
}

func StatusFrom(status string) Status {
	switch status {
	case StatusSkipped.String():
		return StatusSkipped
	case StatusValidationFailure.String():
		return StatusValidationFailure
	case StatusValidationSuccess.String():
		return StatusValidationSuccess
	case StatusToCreate.String():
		return StatusToCreate
	case StatusToUpdate.String():
		return StatusToUpdate
	case StatusCreateFailure.String():
		return StatusCreateFailure
	case StatusUpdateFailure.String():
		return StatusUpdateFailure
	case StatusSuccess.String():
		return StatusSuccess
	default:
		return StatusUnknown
	}
}
