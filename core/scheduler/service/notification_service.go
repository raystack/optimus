package service

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/telemetry"
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

func (n *NotifyService) Push(ctx context.Context, event *scheduler.Event) error {
	jobDetails, err := n.jobRepo.GetJobDetails(ctx, event.Tenant.ProjectName(), event.JobName)
	if err != nil {
		n.l.Error("error getting detail for job [%s]: %s", event.JobName, err)
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

				n.l.Debug("notification event for job: %s , event: %+v", event.JobName, event)
				if plainTextSecretsList == nil {
					plainTextSecretsList, err = n.tenantService.GetSecrets(ctx, event.Tenant)
					if err != nil {
						n.l.Error("error getting secrets for project [%s] namespace [%s]: %s",
							event.Tenant.ProjectName().String(), event.Tenant.NamespaceName().String(), err)
						multierror.Append(err)
						continue
					}
					secretMap = tenant.PlainTextSecrets(plainTextSecretsList).ToMap()
				}

				var secret string
				switch scheme {
				case NotificationSchemeSlack:
					secret = secretMap[tenant.SecretNotifySlack]
				case NotificationSchemePagerDuty:
					secret = secretMap[strings.ReplaceAll(route, "#", "notify_")]
				}

				if notifyChannel, ok := n.notifyChannels[scheme]; ok {
					if currErr := notifyChannel.Notify(ctx,
						scheduler.NotifyAttrs{
							Owner:    jobDetails.JobMetadata.Owner,
							JobEvent: event,
							Secret:   secret,
							Route:    route,
						}); currErr != nil {
						n.l.Error("Error: No notification event for job current error: %s", currErr)
						multierror.Append(fmt.Errorf("notifyChannel.Notify: %s: %w", channel, currErr))
					}
				}
			}
			telemetry.NewCounter("jobrun_alerts_total", map[string]string{
				"project":   event.Tenant.ProjectName().String(),
				"namespace": event.Tenant.NamespaceName().String(),
				"type":      event.Type.String(),
			}).Inc()
		}
	}
	return multierror.ToErr()
}

func (n *NotifyService) Close() error {
	me := errors.NewMultiError("ErrorsInNotifyClose")
	for _, notify := range n.notifyChannels {
		if cerr := notify.Close(); cerr != nil {
			n.l.Error("error closing notificication channel: %s", cerr)
			me.Append(cerr)
		}
	}
	return me.ToErr()
}

func NewNotifyService(l log.Logger, jobRepo JobRepository, tenantService TenantService, notifyChan map[string]Notifier) *NotifyService {
	return &NotifyService{
		l:              l,
		jobRepo:        jobRepo,
		tenantService:  tenantService,
		notifyChannels: notifyChan,
	}
}
