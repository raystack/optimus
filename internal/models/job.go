package models

import (
	"errors"
)

var (
	ErrNoSuchSpec  = errors.New("spec not found")
	ErrNoJobs      = errors.New("no job found")
	ErrNoResources = errors.New("no resources found")
)
