package airflow

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/internal/lib/cron"
)

const (
	pageLimit    = 99999
	dagStatusURL = "api/v1/dags/%s/dagRuns"
)

type airflowRequest struct {
	URL    string
	method string
	body   []byte
	param  string
}

type DagRunListResponse struct {
	DagRuns      []DagRun `json:"dag_runs"`
	TotalEntries int      `json:"total_entries"`
}

type DagRun struct {
	ExecutionDate   time.Time `json:"execution_date"`
	State           string    `json:"state"`
	ExternalTrigger bool      `json:"external_trigger"`
}

type DagRunRequest struct {
	OrderBy          string   `json:"order_by"`
	PageOffset       int      `json:"page_offset"`
	PageLimit        int      `json:"page_limit"`
	DagIds           []string `json:"dag_ids"`
	ExecutionDateGte string   `json:"execution_date_gte,omitempty"`
	ExecutionDateLte string   `json:"execution_date_lte,omitempty"`
}

type SchedulerAuth struct {
	host  string
	token string
}

type ClientAirflow struct {
	client *http.Client
}

func NewAirflowClient() *ClientAirflow {
	return &ClientAirflow{client: &http.Client{}}
}

func (ac ClientAirflow) Invoke(ctx context.Context, r airflowRequest, auth SchedulerAuth) ([]byte, error) {
	var resp []byte

	request, err := http.NewRequestWithContext(ctx, r.method, buildEndPoint(auth.host, r.URL, r.param), bytes.NewBuffer(r.body))
	if err != nil {
		return resp, fmt.Errorf("failed to build http request for %s due to %w", r.URL, err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(auth.token))))

	httpResp, respErr := ac.client.Do(request)
	if respErr != nil {
		return resp, fmt.Errorf("failed to call airflow %s due to %w", r.URL, respErr)
	}
	if httpResp.StatusCode != http.StatusOK {
		httpResp.Body.Close()
		return resp, fmt.Errorf("status code received %d on calling %s", httpResp.StatusCode, r.URL)
	}
	return parseResponse(httpResp)
}

func parseResponse(resp *http.Response) ([]byte, error) {
	var body []byte
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return body, fmt.Errorf("failed to read airflow response: %w", err)
	}
	return body, nil
}

func buildEndPoint(host, reqURL, pathParam string) string {
	host = strings.Trim(host, "/")
	u := &url.URL{
		Scheme: "http",
		Host:   host,
	}
	if pathParam != "" {
		u.Path = "/" + strings.ReplaceAll(reqURL, "%s", pathParam)
	} else {
		u.Path = "/" + reqURL
	}
	if reqURL == dagStatusURL {
		params := url.Values{}
		params.Add("limit", "99999")
		u.RawQuery = params.Encode()
	}
	return u.String()
}

func getJobRuns(res DagRunListResponse, spec *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	var jobRunList []*scheduler.JobRunStatus
	if res.TotalEntries > pageLimit {
		return jobRunList, errors.New("total number of entries exceed page limit")
	}
	for _, dag := range res.DagRuns {
		if !dag.ExternalTrigger { // only include scheduled runs
			scheduledAt := spec.Next(dag.ExecutionDate)
			jobRunStatus, _ := scheduler.JobRunStatusFrom(scheduledAt, dag.State)
			// use multi error to collect errors and proceed
			jobRunList = append(jobRunList, &jobRunStatus)
		}
	}
	return jobRunList, nil
}

func startChildSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	tracer := otel.Tracer("scheduler/airflow")

	return tracer.Start(ctx, name)
}
