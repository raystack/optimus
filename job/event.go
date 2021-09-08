package job

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
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
						err = multierror.Append(err, errors.Wrapf(currErr, "notifyChannel.Notify: %s", channel))
					}
				}
			}
		}
	}
	return err
}

func (e *eventService) Close() error {
	var err error
	for _, notify := range e.notifyChannels {
		err = multierror.Append(err, notify.Close())
	}
	return err
}

func NewEventService(lg log.Logger, notifyChan map[string]models.Notifier) *eventService {
	return &eventService{
		log:            lg,
		notifyChannels: notifyChan,
	}
}
