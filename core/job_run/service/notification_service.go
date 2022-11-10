package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/models"
)

type NotifyService struct {
	notifyChannels map[string]models.Notifier
	jobSrv         JobServiceForNotifier
	l              log.Logger
}

type JobServiceForNotifier interface {
	GetNotificationConfig(ctx context.Context, tnnt tenant.Tenant, jobName job_run.JobName) ([]job_run.JobNotifierConfig, error)
}

func (n NotifyService) Push(ctx context.Context, event job_run.Event) error {

	notificationConfig, err := n.jobSrv.GetNotificationConfig(ctx, event.Tenant, event.JobName)
	for _, notify := range notificationConfig {
		if event.Type.IsOfType(notify.On) {
			for _, channel := range notify.Channels {
				chanParts := strings.Split(channel, "://")
				scheme := chanParts[0]
				route := chanParts[1]

				n.l.Debug("notification event for job", "job spec name", event.JobName, "event", fmt.Sprintf("%v", event))
				if notifyChannel, ok := n.notifyChannels[scheme]; ok {
					if currErr := notifyChannel.Notify(ctx, models.NotifyAttrs{
						Namespace: event.Tenant.NamespaceName(),
						JobSpec:   jobSpec,
						JobEvent:  evt,
						Route:     route,
					}); currErr != nil {
						e.log.Error("Error: No notification event for job ", "current error", currErr)
						err = multierror.Append(err, fmt.Errorf("notifyChannel.Notify: %s: %w", channel, currErr))
					}
				}
			}
		}
	}
	if evt.Type == models.JobFailureEvent {
		jobFailureCounter.Inc()
	} else if evt.Type == models.SLAMissEvent {
		jobSLAMissCounter.Inc()
	}
	return err
}

func NewNotifyService(l log.Logger, jobService JobServiceForNotifier, notifyChan map[string]models.Notifier) *NotifyService {
	return &NotifyService{
		l:              l,
		jobSrv:         jobService,
		notifyChannels: notifyChan,
	}
}
