package pagerduty

import (
	"context"
	"encoding/json"

	"github.com/PagerDuty/go-pagerduty"
)

type PagerDutyService interface {
	SendAlert(context.Context, Event) error
}

type PagerDutyServiceImpl struct {
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

func (*PagerDutyServiceImpl) SendAlert(ctx context.Context, evt Event) error {
	details, err := buildPayloadCustomDetails(evt)
	if err != nil {
		return err
	}

	payload := pagerduty.V2Payload{
		Summary:  "Optimus " + string(evt.meta.Type) + " " + evt.jobName,
		Severity: "critical",
		Source:   evt.projectName,
		Details:  details,
	}

	e := pagerduty.V2Event{
		RoutingKey: evt.routingKey,
		Action:     "trigger",
		Payload:    &payload,
	}
	_, err = pagerduty.ManageEventWithContext(ctx, e)

	if err != nil {
		return err
	}

	return nil
}
