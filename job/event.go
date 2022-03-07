package job

import (
	"context"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/hashicorp/go-multierror"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
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

type eventService struct {
	// scheme -> notifier
	notifyChannels map[string]models.Notifier
	log            log.Logger
}

func (e *eventService) Register(ctx context.Context, namespace models.NamespaceSpec, jobSpec models.JobSpec,
	evt models.JobEvent) error {
	var err error
	for _, notify := range jobSpec.Behavior.Notify {
		if notify.On == evt.Type {
			for _, channel := range notify.Channels {
				chanParts := strings.Split(channel, "://")
				scheme := chanParts[0]
				route := chanParts[1]

				e.log.Debug("notification event for job", "job spec name", jobSpec.Name, "event", fmt.Sprintf("%v", evt))
				if notifyChannel, ok := e.notifyChannels[scheme]; ok {
					if currErr := notifyChannel.Notify(ctx, models.NotifyAttrs{
						Namespace: namespace,
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
	if evt.Type == models.JobEventTypeFailure {
		jobFailureCounter.Inc()
	} else if evt.Type == models.JobEventTypeSLAMiss {
		jobSLAMissCounter.Inc()
	}
	return err
}

func (e *eventService) Close() error {
	var err error
	for _, notify := range e.notifyChannels {
		if cerr := notify.Close(); cerr != nil {
			err = multierror.Append(err, cerr)
		}
	}
	return err
}

func NewEventService(lg log.Logger, notifyChan map[string]models.Notifier) *eventService {
	return &eventService{
		log:            lg,
		notifyChannels: notifyChan,
	}
}
