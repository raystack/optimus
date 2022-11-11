package slack

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	api "github.com/slack-go/slack"

	"github.com/odpf/optimus/core/job_run"
)

const (
	OAuthTokenSecretName      = "NOTIFY_SLACK"
	DefaultEventBatchInterval = time.Second * 10
	MaxSLAEventsToProcess     = 6
)

var (
	slackQueueCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "notify_slack_queue",
		Help: "Items queued in slack notification channel",
	})
	slackWorkerBatchCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "notify_slack_worker_batch",
		Help: "Worker execution count in slack notification channel",
	})
	slackWorkerSendErrCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "notify_slack_worker_send_err",
		Help: "Failure of messages in slack notification channel worker",
	})
)

type Notifier struct {
	io.Closer

	slackURL      string
	routeMsgBatch map[route][]event // channelID -> [][][][][]
	wg            sync.WaitGroup
	mu            sync.Mutex
	workerErrChan chan error

	eventBatchInterval time.Duration
}

type route struct {
	receiverID string
	authToken  string
}

type event struct {
	authToken     string
	projectName   string
	namespaceName string
	jobName       string
	owner         string
	meta          job_run.Event
}

func (s *Notifier) Notify(ctx context.Context, attr job_run.NotifyAttrs) error {

	client := api.New(attr.Secret, api.OptionAPIURL(s.slackURL))

	var receiverIDs []string

	// channel
	if strings.HasPrefix(attr.Route, "#") {
		receiverIDs = append(receiverIDs, attr.Route)
	}

	// user
	if strings.Contains(attr.Route, "@") {
		if strings.HasPrefix(attr.Route, "@") {
			// user group
			groupHandle := strings.TrimLeft(attr.Route, "@")
			groups, err := client.GetUserGroupsContext(ctx)
			if err != nil {
				return fmt.Errorf("client.GetUserGroupsContext: %w", err)
			}
			var groupID string
			for _, group := range groups {
				if group.Handle == groupHandle {
					groupID = group.ID
					break
				}
			}
			receiverIDs, err = client.GetUserGroupMembersContext(ctx, groupID)
			if err != nil {
				return fmt.Errorf("client.GetUserGroupMembersContext: %w", err)
			}
		} else {
			// user email
			user, err := client.GetUserByEmail(attr.Route)
			if err != nil {
				return fmt.Errorf("client.GetUserByEmail: %w", err)
			}
			receiverIDs = append(receiverIDs, user.ID)
		}
	}

	// fail if unable to find the receiver ID
	if len(receiverIDs) == 0 {
		return fmt.Errorf("failed to find notification route %s", attr.Route)
	}

	s.queueNotification(receiverIDs, attr.Secret, attr)
	return nil
}

func (s *Notifier) queueNotification(receiverIDs []string, oauthSecret string, attr job_run.NotifyAttrs) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, receiverID := range receiverIDs {
		rt := route{
			receiverID: receiverID,
			authToken:  oauthSecret,
		}
		if _, ok := s.routeMsgBatch[rt]; !ok {
			s.routeMsgBatch[rt] = []event{}
		}

		evt := event{
			authToken:     oauthSecret,
			projectName:   attr.JobEvent.Tenant.ProjectName().String(),
			namespaceName: attr.JobEvent.Tenant.NamespaceName().String(),
			jobName:       attr.JobEvent.JobName.String(),
			owner:         attr.Owner,
			meta:          attr.JobEvent,
		}
		s.routeMsgBatch[rt] = append(s.routeMsgBatch[rt], evt)
	}
	slackQueueCounter.Inc()
}

// accumulate messages
func buildMessageBlocks(events []event, workerErrChan chan error) []api.Block {
	var blocks []api.Block

	// core details related to event
	for evtIdx, evt := range events {
		fieldSlice := make([]*api.TextBlockObject, 0)
		fieldSlice = append(fieldSlice, api.NewTextBlockObject("mrkdwn", fmt.Sprintf("*Job:*\n%s", evt.jobName), false, false))
		fieldSlice = append(fieldSlice, api.NewTextBlockObject("mrkdwn", fmt.Sprintf("*Owner:*\n%s", evt.owner), false, false))

		if evt.meta.Type.IsOfType(job_run.EventCategorySLAMiss) {
			heading := api.NewTextBlockObject("plain_text",
				fmt.Sprintf("[Job] SLA Breached | %s/%s", evt.projectName, evt.namespaceName), true, false)
			blocks = append(blocks, api.NewHeaderBlock(heading))

			if slas, ok := evt.meta.Values["slas"]; ok {
				for slaIdx, sla := range slas.([]any) {
					slaFields := sla.(map[string]any)
					slaStr := ""
					if taskID, ok := slaFields["task_id"]; ok {
						slaStr += "\nTask: " + taskID.(string)
					}
					if scheduledAt, ok := slaFields["scheduled_at"]; ok {
						slaStr += "\nScheduled at: " + scheduledAt.(string)
					}
					if slaStr != "" {
						if slaIdx > MaxSLAEventsToProcess {
							slaStr += "\nToo many breaches. Truncating..."
						}
						fieldSlice = append(fieldSlice, api.NewTextBlockObject("mrkdwn", fmt.Sprintf("*Breached item:*%s", slaStr), false, false))
					}

					// skip further SLA events
					if slaIdx > MaxSLAEventsToProcess {
						break
					}
				}
			}
		} else if evt.meta.Type.IsOfType(job_run.EventCategoryJobFailure) {
			heading := api.NewTextBlockObject("plain_text",
				fmt.Sprintf("[Job] Failure | %s/%s", evt.projectName, evt.namespaceName), true, false)
			blocks = append(blocks, api.NewHeaderBlock(heading))

			if scheduledAt, ok := evt.meta.Values["scheduled_at"]; ok && scheduledAt.(string) != "" {
				fieldSlice = append(fieldSlice, api.NewTextBlockObject("mrkdwn", fmt.Sprintf("*Scheduled At:*\n%s", scheduledAt.(string)), false, false))
			}
			if duration, ok := evt.meta.Values["duration"]; ok && duration.(string) != "" {
				fieldSlice = append(fieldSlice, api.NewTextBlockObject("mrkdwn", fmt.Sprintf("*Duration:*\n%s", duration.(string)), false, false))
			}
			if taskID, ok := evt.meta.Values["task_id"]; ok && taskID.(string) != "" {
				fieldSlice = append(fieldSlice, api.NewTextBlockObject("mrkdwn", fmt.Sprintf("*Task ID:*\n%s", taskID.(string)), false, false))
			}
		} else {
			workerErrChan <- fmt.Errorf("worker_buildMessageBlocks: unknown event type: %v", evt.meta.Type)
			continue
		}

		fieldsSection := api.NewSectionBlock(nil, fieldSlice, nil)
		blocks = append(blocks, fieldsSection)

		// event log url button
		if logURL, ok := evt.meta.Values["log_url"]; ok && logURL.(string) != "" {
			logText := api.NewTextBlockObject("plain_text", "View log :memo:", true, false)
			logElement := api.NewButtonBlockElement("", "view_log", logText).WithStyle(api.StyleDanger)
			logElement.URL = logURL.(string)
			blocks = append(blocks, api.NewActionBlock("", logElement))
		}

		// event job url button
		if jobURL, ok := evt.meta.Values["job_url"]; ok && jobURL.(string) != "" {
			logText := api.NewTextBlockObject("plain_text", "View job :memo:", true, false)
			logElement := api.NewButtonBlockElement("", "view_job", logText).WithStyle(api.StyleDanger)
			logElement.URL = jobURL.(string)
			blocks = append(blocks, api.NewActionBlock("", logElement))
		}

		// build context footer
		var detailsElementsSlice []api.MixedElement
		if exception, ok := evt.meta.Values["exception"]; ok && exception.(string) != "" {
			optionText := api.NewTextBlockObject("plain_text", fmt.Sprintf("Exception:\n%s", exception.(string)), true, false)
			detailsElementsSlice = append(detailsElementsSlice, optionText) // api.NewOptionBlockObject("", optionText, nil))
		}
		if message, ok := evt.meta.Values["message"]; ok && message.(string) != "" {
			optionText := api.NewTextBlockObject("plain_text", fmt.Sprintf("Message:\n%s", message.(string)), true, false)
			detailsElementsSlice = append(detailsElementsSlice, optionText)
		}
		if len(detailsElementsSlice) > 0 {
			// Build context section
			blocks = append(blocks, api.NewContextBlock("", detailsElementsSlice...))
		}

		if len(events) != evtIdx+1 {
			blocks = append(blocks, api.NewDividerBlock())
		}
	}
	return blocks
}

func (s *Notifier) Worker(ctx context.Context) {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		// iterate over all queued routeMsgBatch and
		for route, events := range s.routeMsgBatch {
			if len(events) == 0 {
				continue
			}
			var messageOptions []api.MsgOption
			messageOptions = append(messageOptions, api.MsgOptionBlocks(buildMessageBlocks(events, s.workerErrChan)...))
			messageOptions = append(messageOptions, api.MsgOptionAsUser(true))

			client := api.New(route.authToken, api.OptionAPIURL(s.slackURL))
			if _, _, _, err := client.SendMessage(route.receiverID,
				messageOptions...,
			); err != nil {
				cleanedEvents := []event{}
				for _, ev := range events {
					ev.authToken = "*redacted*"
					cleanedEvents = append(cleanedEvents, ev)
				}
				s.workerErrChan <- fmt.Errorf("worker_sendMessageContext: %v: %w", cleanedEvents, err)
			}

			// clear events from map as they are processed
			s.routeMsgBatch[route] = []event{}
		}
		s.mu.Unlock()

		slackWorkerBatchCounter.Inc()
		select {
		case <-ctx.Done():
			close(s.workerErrChan)
			return
		default:
			// send messages in batches of 5 secs
			time.Sleep(s.eventBatchInterval)
		}
	}
}

func (s *Notifier) Close() error { // nolint: unparam
	// drain batches
	s.wg.Wait()
	return nil
}

func NewNotifier(ctx context.Context, slackURL string, eventBatchInterval time.Duration, errHandler func(error)) *Notifier {
	this := &Notifier{
		slackURL:           slackURL,
		routeMsgBatch:      map[route][]event{},
		workerErrChan:      make(chan error),
		eventBatchInterval: eventBatchInterval,
	}

	this.wg.Add(1)
	go func() {
		for err := range this.workerErrChan {
			errHandler(err)
			slackWorkerSendErrCounter.Inc()
		}
		this.wg.Done()
	}()

	this.wg.Add(1)
	go this.Worker(ctx)
	return this
}
