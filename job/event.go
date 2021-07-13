package job

import (
	"context"
	"strings"

	log "github.com/odpf/optimus/core/logger"

	"github.com/pkg/errors"

	"github.com/hashicorp/go-multierror"

	"github.com/odpf/optimus/models"
)

type eventService struct {
	// scheme -> notifier
	notifyChannels map[string]models.Notifier
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

				log.Df("notification event for job %s: %v", jobSpec.Name, evt)
				if notifyChannel, ok := e.notifyChannels[scheme]; ok {
					if currErr := notifyChannel.Notify(ctx, models.NotifyAttrs{
						Namespace: namespace,
						JobSpec:   jobSpec,
						JobEvent:  evt,
						Route:     route,
					}); currErr != nil {
						log.E(currErr)
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

func NewEventService(notifyChan map[string]models.Notifier) *eventService {
	return &eventService{
		notifyChannels: notifyChan,
	}
}
