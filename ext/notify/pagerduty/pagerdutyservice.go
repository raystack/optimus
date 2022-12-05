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
	details := &customDetails{Owner: evt.owner, Namespace: evt.meta.Tenant.NamespaceName().String()}
	if logURL, ok := evt.meta.Values["log_url"]; ok && logURL.(string) != "" {
		details.LogURL = logURL.(string)
	}
	if jobURL, ok := evt.meta.Values["job_url"]; ok && jobURL.(string) != "" {
		details.JobURL = jobURL.(string)
	}
	if exception, ok := evt.meta.Values["exception"]; ok && exception.(string) != "" {
		details.Exception = exception.(string)
	}
	if message, ok := evt.meta.Values["message"]; ok && message.(string) != "" {
		details.Message = message.(string)
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
		Summary:  "Optimus " + string(evt.meta.Type) + " " + evt.meta.JobName.String(),
		Severity: "critical",
		Source:   evt.meta.Tenant.ProjectName().String(),
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
