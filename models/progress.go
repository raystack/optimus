package models

import "fmt"

const (
	ProgressTypeJobSpecUnknownDependencyUsed = "unknown dependency used"
	ProgressTypeJobDependencyResolution      = "dependency resolution"
	ProgressTypeJobUpload                    = "job upload"
	ProgressTypeJobDeploymentRequestCreated  = "job deployment request created"
)

type (
	// ProgressJobSpecFetch represents a specification being
	// read from the storage
	ProgressJobSpecFetch struct{}

	// ProgressJobDependencyResolutionFinished represents dependencies are being
	// successfully resolved
	ProgressJobDependencyResolutionFinished struct{}

	// ProgressJobSpecUnknownDependencyUsed represents a job spec has used
	// dependencies which are unknown/unresolved
	ProgressJobSpecUnknownDependencyUsed struct {
		Job        string
		Dependency string
	}

	// ProgressJobDependencyResolution represents a job dependency is failed to be
	// refreshed thus deployed using the persisted dependencies
	ProgressJobDependencyResolution struct {
		Job string
		Err error
	}

	// ProgressJobDependencyFetch represents dependencies are being read from the storage
	ProgressJobDependencyFetch struct{}

	// ProgressJobSpecWithDependencyFetch represents job specs with dependencies have been fetched
	ProgressJobSpecWithDependencyFetch struct{}

	// ProgressSavedJobDelete signifies that a raw
	// job from a repository is being deleted
	ProgressSavedJobDelete struct{ Name string }

	// ProgressJobPriorityWeightAssign signifies that a
	// job is being assigned a priority weight

	ProgressJobPriorityWeightAssign struct{}
	// ProgressJobPriorityWeightAssignmentFailed signifies that a
	// job is failed during priority weight assignment

	ProgressJobPriorityWeightAssignmentFailed struct {
		Err error
	}

	// ProgressJobCheckFailed represents if a job is not valid
	ProgressJobCheckFailed struct {
		Name   string
		Reason string
	}

	// ProgressJobCheckSuccess represents a job is valid
	ProgressJobCheckSuccess struct {
		Name string
	}

	// ProgressJobSpecCompiled represents a specification
	// being compiled to a Job
	ProgressJobSpecCompiled struct{ Name string }

	// ProgressJobUpload represents the compiled Job
	// being uploaded
	ProgressJobUpload struct {
		Name string
		Err  error
	}

	// ProgressJobRemoteDelete signifies that a
	// compiled job from a remote repository is being deleted
	ProgressJobRemoteDelete struct{ Name string }

	// ProgressJobSpecHookDependencyEnrich represents job specs have been enriched with the hook dependencies
	ProgressJobSpecHookDependencyEnrich struct{}

	// ProgressJobDeploymentRequestCreated represents a job deployment has been requested
	ProgressJobDeploymentRequestCreated struct {
		DeployID DeploymentID
	}
)

func (e *ProgressJobSpecFetch) String() string {
	return "fetching job specs"
}

func (e *ProgressSavedJobDelete) String() string {
	return fmt.Sprintf("deleting: %s", e.Name)
}

func (e *ProgressJobPriorityWeightAssign) String() string {
	return "assigned priority weights"
}

func (e *ProgressJobPriorityWeightAssignmentFailed) String() string {
	return fmt.Sprintf("failed priority weight assignment: %v", e.Err)
}

func (e *ProgressJobDependencyResolutionFinished) String() string {
	return "dependencies resolved"
}

func (e *ProgressJobSpecUnknownDependencyUsed) String() string {
	return fmt.Sprintf("could not find registered destination '%s' during compiling dependencies for the provided job %s", e.Dependency, e.Job)
}

func (e *ProgressJobSpecUnknownDependencyUsed) Type() string {
	return ProgressTypeJobSpecUnknownDependencyUsed
}

func (e *ProgressJobDependencyResolution) String() string {
	if e.Err != nil {
		return fmt.Sprintf("failed to resolve job dependencies of '%s': %s", e.Job, e.Err)
	}
	return fmt.Sprintf("resolved job dependencies of '%s'", e.Job)
}

func (e *ProgressJobDependencyResolution) Type() string {
	return ProgressTypeJobDependencyResolution
}

func (e *ProgressJobDependencyFetch) String() string {
	return "job dependencies has been fetched"
}

func (e *ProgressJobSpecWithDependencyFetch) String() string {
	return "job specs with job dependencies has been fetched"
}

func (e *ProgressJobCheckFailed) String() string {
	return fmt.Sprintf("check for job failed: %s, reason: %s", e.Name, e.Reason)
}

func (e *ProgressJobCheckSuccess) String() string {
	return fmt.Sprintf("check for job passed: %s", e.Name)
}

func (e *ProgressJobSpecCompiled) String() string {
	return fmt.Sprintf("compiling: %s", e.Name)
}

func (e *ProgressJobUpload) String() string {
	if e.Err != nil {
		return fmt.Sprintf("uploading: %s, failed with error): %s", e.Name, e.Err.Error())
	}
	return fmt.Sprintf("uploaded: %s", e.Name)
}

func (e *ProgressJobUpload) Type() string {
	return ProgressTypeJobUpload
}

func (e *ProgressJobRemoteDelete) String() string {
	return fmt.Sprintf("deleting: %s", e.Name)
}

func (e *ProgressJobSpecHookDependencyEnrich) String() string {
	return "jobs enriched with hook dependencies"
}

func (e *ProgressJobDeploymentRequestCreated) String() string {
	return "job deployment requested"
}

func (e *ProgressJobDeploymentRequestCreated) ID() DeploymentID {
	return e.DeployID
}

func (e *ProgressJobDeploymentRequestCreated) Type() string {
	return ProgressTypeJobDeploymentRequestCreated
}
