package moderator

import (
	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var eventQueueCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "publisher_events_created_counter",
	Help: "Events created and to be sent to writer",
})

type Event interface {
	Bytes() ([]byte, error)
}

type EventHandler struct {
	messageChan chan<- []byte
	logger      log.Logger
}

func NewEventHandler(messageChan chan<- []byte, logger log.Logger) *EventHandler {
	return &EventHandler{
		messageChan: messageChan,
		logger:      logger,
	}
}

func (e EventHandler) HandleEvent(event Event) {
	if e.messageChan == nil {
		e.logger.Warn("event is not published because it is not configured")
		return
	}

	bytes, err := event.Bytes()
	if err != nil {
		e.logger.Error("error converting event to bytes: %v", err)
		return
	}

	go func() { e.messageChan <- bytes }()
	eventQueueCounter.Inc()
}
