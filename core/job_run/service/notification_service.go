package service

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/odpf/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/models"
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
	Notify(ctx context.Context, attr job_run.NotifyAttrs) error
}

type NotifyService struct {
	notifyChannels map[string]models.Notifier
	jobRepo        JobRepository
	tenantService  TenantService
	l              log.Logger
}

func (n NotifyService) Push(ctx context.Context, event job_run.Event, jobOwner string) error {

	jobDetails, err := n.jobRepo.GetJobDetails(ctx, event.Tenant, event.JobName)
	notificationConfig := jobDetails.Alerts
	for _, notify := range notificationConfig {
		if event.Type.IsOfType(notify.On) {
			for _, channel := range notify.Channels {
				chanParts := strings.Split(channel, "://")
				scheme := chanParts[0]
				route := chanParts[1]

				n.l.Debug("notification event for job", "job spec name", event.JobName, "event", fmt.Sprintf("%v", event))

				plainTextSecretsList, err := n.tenantService.GetSecrets(ctx,
					event.Tenant.ProjectName(),
					event.Tenant.NamespaceName().String())
				if err != nil {
					return err
				}

				secretMap := SecretsToMap(plainTextSecretsList)
				var secret string
				switch scheme {
				case NotificationSchemeSlack:
					secret = secretMap["NOTIFY_SLACK"]
				case NotificationSchemePagerDuty:
					secret = secretMap[strings.ReplaceAll(route, "#", "notify_")]
				}

				if notifyChannel, ok := n.notifyChannels[scheme]; ok {
					if currErr := notifyChannel.Notify(ctx, job_run.NotifyAttrs{
						Owner:    jobOwner,
						JobEvent: event,
						Secret:   secret,
						Route:    route,
					}); currErr != nil {
						n.l.Error("Error: No notification event for job ", "current error", currErr)
						err = multierror.Append(err, fmt.Errorf("notifyChannel.Notify: %s: %w", channel, currErr))
					}
				}
			}
		}
	}
	if event.Type.IsOfType(job_run.EventCategoryJobFailure) {
		jobFailureCounter.Inc()
	} else if event.Type.IsOfType(job_run.EventCategorySLAMiss) {
		jobSLAMissCounter.Inc()
	}
	return err
}

func NewNotifyService(l log.Logger, jobRepo JobRepository, tenantService TenantService, notifyChan map[string]models.Notifier) *NotifyService {
	return &NotifyService{
		l:              l,
		jobRepo:        jobRepo,
		tenantService:  tenantService,
		notifyChannels: notifyChan,
	}
}
