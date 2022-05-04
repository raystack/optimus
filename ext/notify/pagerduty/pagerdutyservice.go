package pagerduty

import (
	"context"

	"github.com/PagerDuty/go-pagerduty"
)

type PagerDutyService interface {
	SendPagerDutyAlert(context.Context, Event) error
}

type PagerDutyServiceImpl struct {
}

func (*PagerDutyServiceImpl) SendPagerDutyAlert(ctx context.Context, evt Event) error {
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
