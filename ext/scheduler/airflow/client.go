package airflow

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
)

const (
	pageLimit = 99999
)

type airflowRequest struct {
	path   string
	method string
	body   []byte
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

	endpoint := buildEndPoint(auth.host, r.path)
	request, err := http.NewRequestWithContext(ctx, r.method, endpoint, bytes.NewBuffer(r.body))
	if err != nil {
		return resp, fmt.Errorf("failed to build http request for %s due to %w", endpoint, err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(auth.token))))

	httpResp, respErr := ac.client.Do(request)
	if respErr != nil {
		return resp, fmt.Errorf("failed to call airflow %s due to %w", endpoint, respErr)
	}
	if httpResp.StatusCode != http.StatusOK {
		httpResp.Body.Close()
		return resp, fmt.Errorf("status code received %d on calling %s", httpResp.StatusCode, endpoint)
	}
	return parseResponse(httpResp)
}

func parseResponse(resp *http.Response) ([]byte, error) {
	var body []byte
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return body, errors.Wrap(EntityAirflow, "failed to read airflow response", err)
	}
	return body, nil
}

func buildEndPoint(host, path string) string {
	host = strings.Trim(host, "/")
	u := &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   path,
	}
	return u.String()
}

func getJobRuns(res DagRunListResponse, spec *cron.ScheduleSpec, withExternalTrigger bool) ([]*scheduler.JobRunStatus, error) {
	var jobRunList []*scheduler.JobRunStatus
	if res.TotalEntries > pageLimit {
		return jobRunList, errors.InternalError(EntityAirflow, "total number of entries exceed page limit", nil)
	}
	for _, dag := range res.DagRuns {
		if withExternalTrigger || !dag.ExternalTrigger {
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
