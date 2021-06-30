package slack

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	api "github.com/slack-go/slack"

	"github.com/odpf/optimus/models"
)

const (
	OAuthTokenSecretName = "NOTIFY_SLACK"

	DefaultEventBatchInterval = time.Second * 10

	MaxSLAEventsToProcess = 6
)

type Notifier struct {
	io.Closer

	slackUrl      string
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
	meta          models.JobEvent
}

func (s *Notifier) Notify(ctx context.Context, attr models.NotifyAttrs) error {
	oauthSecret, ok := attr.Namespace.ProjectSpec.Secret.GetByName(OAuthTokenSecretName)
	if !ok {
		return errors.Errorf("failed to find authentication token of bot required for sending notifications, please register %s secret", OAuthTokenSecretName)
	}
	client := api.New(oauthSecret, api.OptionAPIURL(s.slackUrl))

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
				return errors.Wrapf(err, "client.GetUserGroupsContext")
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
				return errors.Wrapf(err, "client.GetUserGroupMembersContext")
			}
		} else {
			// user email
			user, err := client.GetUserByEmail(attr.Route)
			if err != nil {
				return errors.Wrapf(err, "client.GetUserByEmail")
			}
			receiverIDs = append(receiverIDs, user.ID)
		}
	}

	// fail if unable to find the receiver ID
	if len(receiverIDs) == 0 {
		return errors.Errorf("failed to find notification route %s", attr.Route)
	}

	s.queueNotification(receiverIDs, oauthSecret, attr)
	return nil
}

func (s *Notifier) queueNotification(receiverIDs []string, oauthSecret string, attr models.NotifyAttrs) {
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
			projectName:   attr.Namespace.ProjectSpec.Name,
			namespaceName: attr.Namespace.Name,
			jobName:       attr.JobSpec.Name,
			owner:         attr.JobSpec.Owner,
			meta:          attr.JobEvent,
		}
		s.routeMsgBatch[rt] = append(s.routeMsgBatch[rt], evt)
	}
}

// accumulate messages
func buildMessageBlocks(events []event) []api.Block {
	var blocks []api.Block

	// core details related to event
	for evtIdx, evt := range events {
		fieldSlice := make([]*api.TextBlockObject, 0)
		fieldSlice = append(fieldSlice, api.NewTextBlockObject("mrkdwn", fmt.Sprintf("*Job:*\n%s", evt.jobName), false, false))
		fieldSlice = append(fieldSlice, api.NewTextBlockObject("mrkdwn", fmt.Sprintf("*Owner:*\n%s", evt.owner), false, false))

		switch evt.meta.Type {
		case models.JobEventTypeSLAMiss:
			heading := api.NewTextBlockObject("plain_text",
				fmt.Sprintf("[Job] SLA Breached | %s/%s", evt.projectName, evt.namespaceName), true, false)
			blocks = append(blocks, api.NewHeaderBlock(heading))

			if slas, ok := evt.meta.Value["slas"]; ok {
				for slaIdx, sla := range slas.GetListValue().GetValues() {
					slaFields := sla.GetStructValue().GetFields()
					var slaStr = ""
					if taskID, ok := slaFields["task_id"]; ok {
						slaStr += "\nTask: " + taskID.GetStringValue()
					}
					if scheduledAt, ok := slaFields["scheduled_at"]; ok {
						slaStr += "\nScheduled at: " + scheduledAt.GetStringValue()
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
		case models.JobEventTypeFailure:
			heading := api.NewTextBlockObject("plain_text",
				fmt.Sprintf("[Job] Failure | %s/%s", evt.projectName, evt.namespaceName), true, false)
			blocks = append(blocks, api.NewHeaderBlock(heading))

			if scheduledAt, ok := evt.meta.Value["scheduled_at"]; ok && scheduledAt.GetStringValue() != "" {
				fieldSlice = append(fieldSlice, api.NewTextBlockObject("mrkdwn", fmt.Sprintf("*Scheduled At:*\n%s", scheduledAt.GetStringValue()), false, false))
			}
			if duration, ok := evt.meta.Value["duration"]; ok && duration.GetStringValue() != "" {
				fieldSlice = append(fieldSlice, api.NewTextBlockObject("mrkdwn", fmt.Sprintf("*Duration:*\n%s", duration.GetStringValue()), false, false))
			}
			if taskID, ok := evt.meta.Value["task_id"]; ok && taskID.GetStringValue() != "" {
				fieldSlice = append(fieldSlice, api.NewTextBlockObject("mrkdwn", fmt.Sprintf("*Task ID:*\n%s", taskID.GetStringValue()), false, false))
			}
		default:
			// unknown event
			continue
		}

		fieldsSection := api.NewSectionBlock(nil, fieldSlice, nil)
		blocks = append(blocks, fieldsSection)

		// event log url button
		if logURL, ok := evt.meta.Value["log_url"]; ok && logURL.GetStringValue() != "" {
			logText := api.NewTextBlockObject("plain_text", "View log :memo:", true, false)
			logElement := api.NewButtonBlockElement("", "view_log", logText).WithStyle(api.StyleDanger)
			logElement.URL = logURL.GetStringValue()
			blocks = append(blocks, api.NewActionBlock("", logElement))
		}

		// event job url button
		if jobURL, ok := evt.meta.Value["job_url"]; ok && jobURL.GetStringValue() != "" {
			logText := api.NewTextBlockObject("plain_text", "View job :memo:", true, false)
			logElement := api.NewButtonBlockElement("", "view_job", logText).WithStyle(api.StyleDanger)
			logElement.URL = jobURL.GetStringValue()
			blocks = append(blocks, api.NewActionBlock("", logElement))
		}

		// build context footer
		var detailsElementsSlice []api.MixedElement
		if exception, ok := evt.meta.Value["exception"]; ok && exception.GetStringValue() != "" {
			optionText := api.NewTextBlockObject("plain_text", fmt.Sprintf("Exception:\n%s", exception.GetStringValue()), true, false)
			detailsElementsSlice = append(detailsElementsSlice, optionText) //api.NewOptionBlockObject("", optionText, nil))
		}
		if message, ok := evt.meta.Value["message"]; ok && message.GetStringValue() != "" {
			optionText := api.NewTextBlockObject("plain_text", fmt.Sprintf("Message:\n%s", message.GetStringValue()), true, false)
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
			messageOptions = append(messageOptions, api.MsgOptionBlocks(buildMessageBlocks(events)...))
			messageOptions = append(messageOptions, api.MsgOptionAsUser(true))

			client := api.New(route.authToken, api.OptionAPIURL(s.slackUrl))
			if _, _, _, err := client.SendMessage(route.receiverID,
				messageOptions...,
			); err != nil {
				cleanedEvents := []event{}
				for _, ev := range events {
					ev.authToken = "*redacted*"
					cleanedEvents = append(cleanedEvents, ev)
				}
				s.workerErrChan <- errors.Wrapf(err, "Worker_SendMessageContext: %v", cleanedEvents)
			}

			// clear events from map as they are processed
			s.routeMsgBatch[route] = []event{}
		}
		s.mu.Unlock()

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

func (s *Notifier) Close() error {
	// drain batches
	s.wg.Wait()
	return nil
}

func NewNotifier(ctx context.Context, slackUrl string, eventBatchInterval time.Duration, errHandler func(error)) *Notifier {
	this := &Notifier{
		slackUrl:           slackUrl,
		routeMsgBatch:      map[route][]event{},
		workerErrChan:      make(chan error, 0),
		eventBatchInterval: eventBatchInterval,
	}

	this.wg.Add(1)
	go func() {
		for err := range this.workerErrChan {
			errHandler(err)
		}
		this.wg.Done()
	}()

	this.wg.Add(1)
	go this.Worker(ctx)
	return this
}
