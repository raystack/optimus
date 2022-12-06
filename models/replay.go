package models

const (
	// ReplayStatusAccepted worker picked up the request
	ReplayStatusAccepted   = "accepted"
	ReplayStatusInProgress = "inprogress"
	// ReplayStatusReplayed worker finished clear up the run instances
	ReplayStatusReplayed = "replayed"
	// ReplayStatusFailed worker fail while processing the replay request
	ReplayStatusFailed    = "failed"    // end state
	ReplayStatusSuccess   = "success"   // end state
	ReplayStatusCancelled = "cancelled" // end state
)
