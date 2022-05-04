package pagerduty

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/odpf/optimus/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"io"
	"strings"
	"sync"
	"time"
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
	routingKey    string
	projectName   string
	namespaceName string
	jobName       string
	owner         string
	meta          models.JobEvent
}

func (s *Notifier) Notify(ctx context.Context, attr models.NotifyAttrs) error {
	routingKey, ok := attr.Namespace.ProjectSpec.Secret.GetByName(strings.ReplaceAll(attr.Route, "#", "notify_"))
	if !ok {
		return fmt.Errorf("failed to find authentication token of bot required for sending notifications, please register %s secret", strings.ReplaceAll(attr.Route, "#", "notify_"))
	}
	s.queueNotification(routingKey, attr)
	return nil
}

func (s *Notifier) queueNotification(routingKey string, attr models.NotifyAttrs) {
	s.mu.Lock()
	defer s.mu.Unlock()
	evt := Event{
		routingKey:    routingKey,
		projectName:   attr.Namespace.ProjectSpec.Name,
		namespaceName: attr.Namespace.Name,
		jobName:       attr.JobSpec.Name,
		owner:         attr.JobSpec.Owner,
		meta:          attr.JobEvent,
	}
	s.msgQueue = append(s.msgQueue, evt)
	pagerdutyQueueCounter.Inc()
}

type customDetails struct {
	Owner     string `json:"owner"`
	Namespace string `json:"namespace"`
	LogURL    string `json:"log_url"`
	JobURL    string `json:"job_url"`
	Exception string `json:"exception"`
	Message   string `json:"message"`
}

func buildPayloadCustomDetails(evt Event) (string, error) {

	details := &customDetails{Owner: evt.owner, Namespace: evt.namespaceName}
	if logURL, ok := evt.meta.Value["log_url"]; ok && logURL.GetStringValue() != "" {
		details.LogURL = logURL.GetStringValue()
	}
	if jobURL, ok := evt.meta.Value["job_url"]; ok && jobURL.GetStringValue() != "" {
		details.JobURL = jobURL.GetStringValue()
	}
	if exception, ok := evt.meta.Value["exception"]; ok && exception.GetStringValue() != "" {
		details.Exception = exception.GetStringValue()
	}
	if message, ok := evt.meta.Value["message"]; ok && message.GetStringValue() != "" {
		details.Message = message.GetStringValue()
	}

	det, err := json.Marshal(&details)
	if err != nil {
		return "", err
	}
	return string(det), nil
}

func (s *Notifier) Worker(ctx context.Context) {
	defer s.wg.Done()

	for {
		s.mu.Lock()
		for _, evt := range s.msgQueue {
			err := s.pdService.SendPagerDutyAlert(ctx, evt)
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
	this := &Notifier{
		msgQueue:           make([]Event, 0),
		workerErrChan:      make(chan error),
		eventBatchInterval: eventBatchInterval,
		pdService:          pdService,
	}

	this.wg.Add(1)
	go func() {
		for err := range this.workerErrChan {
			errHandler(err)
			pagerdutyWorkerSendErrCounter.Inc()
		}
		this.wg.Done()
	}()
	this.wg.Add(1)
	go this.Worker(ctx)
	return this
}
