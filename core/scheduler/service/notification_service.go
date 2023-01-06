package service

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/odpf/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

var (
	jobFailureCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "job_event_failure",
		Help: "Event received for job failures by scheduler",
	})
	jobSLAMissCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "job_event_slamiss",
		Help: "Event received for SLA miss by scheduler",
	})
)

const (
	NotificationSchemeSlack     = "slack"
	NotificationSchemePagerDuty = "pagerduty"
)

type Notifier interface {
	io.Closer
	Notify(ctx context.Context, attr scheduler.NotifyAttrs) error
}

type NotifyService struct {
	notifyChannels map[string]Notifier
	jobRepo        JobRepository
	tenantService  TenantService
	l              log.Logger
}

func (n NotifyService) Push(ctx context.Context, event scheduler.Event) error {
	jobDetails, err := n.jobRepo.GetJobDetails(ctx, event.Tenant.ProjectName(), event.JobName)
	if err != nil {
		return err
	}
	notificationConfig := jobDetails.Alerts
	multierror := errors.NewMultiError("ErrorsInNotifypush")
	var secretMap map[string]string
	var plainTextSecretsList []*tenant.PlainTextSecret
	for _, notify := range notificationConfig {
		if event.Type.IsOfType(notify.On) {
			for _, channel := range notify.Channels {
				chanParts := strings.Split(channel, "://")
				scheme := chanParts[0]
				route := chanParts[1]

				n.l.Debug("notification event for job", "job spec name", event.JobName, "event", fmt.Sprintf("%v", event))
				if plainTextSecretsList == nil {
					plainTextSecretsList, err = n.tenantService.GetSecrets(ctx,
						event.Tenant)
					if err != nil {
						multierror.Append(err)
						continue
					}
					secretMap = SecretsToMap(plainTextSecretsList)
				}

				var secret string
				switch scheme {
				case NotificationSchemeSlack:
					secret = secretMap[tenant.SecretNotifySlack]
				case NotificationSchemePagerDuty:
					secret = secretMap[strings.ReplaceAll(route, "#", "notify_")]
				}

				if notifyChannel, ok := n.notifyChannels[scheme]; ok {
					if currErr := notifyChannel.Notify(ctx, scheduler.NotifyAttrs{
						Owner:    jobDetails.JobMetadata.Owner,
						JobEvent: event,
						Secret:   secret,
						Route:    route,
					}); currErr != nil {
						n.l.Error("Error: No notification event for job ", "current error", currErr)
						multierror.Append(fmt.Errorf("notifyChannel.Notify: %s: %w", channel, currErr))
					}
				}
			}
		}
	}
	if event.Type.IsOfType(scheduler.EventCategoryJobFailure) {
		jobFailureCounter.Inc()
	} else if event.Type.IsOfType(scheduler.EventCategorySLAMiss) {
		jobSLAMissCounter.Inc()
	}
	return errors.MultiToError(multierror)
}

func (n *NotifyService) Close() error {
	multierror := errors.NewMultiError("ErrorsInNotifyClose")
	for _, notify := range n.notifyChannels {
		if cerr := notify.Close(); cerr != nil {
			multierror.Append(cerr)
		}
	}
	return errors.MultiToError(multierror)
}

func NewNotifyService(l log.Logger, jobRepo JobRepository, tenantService TenantService, notifyChan map[string]Notifier) *NotifyService {
	return &NotifyService{
		l:              l,
		jobRepo:        jobRepo,
		tenantService:  tenantService,
		notifyChannels: notifyChan,
	}
}
