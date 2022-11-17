package job

import (
	"github.com/hashicorp/go-multierror"
	"github.com/odpf/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

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

type eventService struct {
	// scheme -> notifier
	notifyChannels map[string]models.Notifier
	log            log.Logger
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
