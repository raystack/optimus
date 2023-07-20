package moderator

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/raystack/salt/log"
)

var eventQueueCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "publisher_events_created_total",
	Help: "Events created and to be sent to writer",
})

type Event interface {
	Bytes() ([]byte, error)
}

type Handler interface {
	HandleEvent(e Event)
}

type NoOpHandler struct{}

func (NoOpHandler) HandleEvent(_ Event) {}

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
	bytes, err := event.Bytes()
	if err != nil {
		e.logger.Error("error converting event to bytes: %v", err)
		return
	}

	go func() { e.messageChan <- bytes }()
	eventQueueCounter.Inc()
}
