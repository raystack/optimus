package pagerduty

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/core/scheduler"
)

const (
	DefaultEventBatchInterval = time.Second * 10
)

var (
	pagerdutyQueueCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "notify_pagerduty_queue",
		Help: "Items queued in pagerduty notification channel",
	})
	pagerdutyWorkerBatchCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "notify_pagerduty_worker_batch",
		Help: "Worker execution count in pagerduty notification channel",
	})
	pagerdutyWorkerSendErrCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "notify_pagerduty_worker_send_err",
		Help: "Failure of messages in pagerduty notification channel worker",
	})
)

type Notifier struct {
	io.Closer
	msgQueue           []Event
	wg                 sync.WaitGroup
	mu                 sync.Mutex
	workerErrChan      chan error
	pdService          PagerDutyService
	eventBatchInterval time.Duration
}

type Event struct {
	routingKey string
	owner      string
	meta       *scheduler.Event
}

func NewEvent(routingKey, owner string, meta *scheduler.Event) Event {
	return Event{
		routingKey: routingKey,
		owner:      owner,
		meta:       meta,
	}
}

func (s *Notifier) Notify(_ context.Context, attr scheduler.NotifyAttrs) error { //nolint:unparam
	s.queueNotification(attr.Secret, attr)
	return nil
}

func (s *Notifier) queueNotification(routingKey string, attr scheduler.NotifyAttrs) {
	s.mu.Lock()
	defer s.mu.Unlock()
	evt := Event{
		routingKey: routingKey,
		owner:      attr.Owner,
		meta:       attr.JobEvent,
	}
	s.msgQueue = append(s.msgQueue, evt)
	pagerdutyQueueCounter.Inc()
}

func (s *Notifier) Worker(ctx context.Context) {
	defer s.wg.Done()

	for {
		s.mu.Lock()
		for _, evt := range s.msgQueue {
			err := s.pdService.SendAlert(ctx, evt)
			if err != nil {
				s.workerErrChan <- fmt.Errorf("Worker_SendMessageContext: %w", err)
			}
		}
		s.msgQueue = nil // empty the queue
		s.mu.Unlock()

		pagerdutyWorkerBatchCounter.Inc()
		select {
		case <-ctx.Done():
			close(s.workerErrChan)
			return
		default:
			time.Sleep(s.eventBatchInterval)
		}
	}
}

func (s *Notifier) Close() error { // nolint: unparam
	// drain batches
	s.wg.Wait()
	return nil
}

func NewNotifier(ctx context.Context, eventBatchInterval time.Duration, errHandler func(error), pdService PagerDutyService) *Notifier {
	notifier := &Notifier{
		msgQueue:           make([]Event, 0),
		workerErrChan:      make(chan error),
		eventBatchInterval: eventBatchInterval,
		pdService:          pdService,
	}

	notifier.wg.Add(1)
	go func() {
		for err := range notifier.workerErrChan {
			errHandler(err)
			pagerdutyWorkerSendErrCounter.Inc()
		}
		notifier.wg.Done()
	}()
	notifier.wg.Add(1)
	go notifier.Worker(ctx)
	return notifier
}
