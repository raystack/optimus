package prime

import (
	"context"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

func (p *Planner) gcZombieInstances(ctx context.Context, instanceRepo store.InstanceRepository, jobRun models.JobRun) error {
	// check existing states for the instance, garbage collect zombie run
	for _, instance := range jobRun.Instances {
		// if the task is marked as running for too long
		if instance.Status == models.RunStateRunning &&
			instance.UpdatedAt.Add(InstanceRunTimeout).After(p.now()) {
			// heartbeat timeout, mark task zombie
			p.l.Warn("found a zombie instance", "job name", jobRun.Spec.Name, "instance name", instance.Name)

			// kill it just in case its still running
			if err := p.executor.Stop(ctx, &models.ExecutorStopRequest{
				ID: instance.ID.String(),
				// for now we are hardcore, this should be changed to SIGTERM
				Signal: "SIGKILL",
			}); err != nil {
				return err
			}

			// cancel instance
			if err := instanceRepo.UpdateStatus(ctx, instance.ID, models.RunStateFailed); err != nil {
				return err
			}
		}
	}
	return nil
}
