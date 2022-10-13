package service

import (
	"github.com/odpf/optimus/core/job/dto"
	"golang.org/x/net/context"
)

type JobService struct {
	repo JobSpecRepository
}

type JobSpecRepository interface {
	Save(ctx context.Context, jobSpec *dto.JobSpec) error
}

func (j JobService) Add(ctx context.Context, jobSpecs []*dto.JobSpec) error {
	// 1. validation

	// 2. identify job destination

	// 3. identify job sources

	// 4. persist to db
	// TODO: Save can also accept multiple job specs
	for _, jobSpec := range jobSpecs {
		if err := j.repo.Save(ctx, jobSpec); err != nil {
			return err
		}
	}

	// 5. send deployment request to scheduling context

	return nil
}
