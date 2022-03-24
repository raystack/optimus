package airflow2

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

	"github.com/odpf/optimus/core/cron"

	"github.com/odpf/optimus/models"
)

const (
	pageLimit = 99999
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
	DagRunID        string    `json:"dag_run_id"`
	DagID           string    `json:"dag_id"`
	LogicalDate     time.Time `json:"logical_date"`
	ExecutionDate   time.Time `json:"execution_date"`
	StartDate       time.Time `json:"start_date"`
	EndDate         time.Time `json:"end_date"`
	State           string    `json:"state"`
	ExternalTrigger bool      `json:"external_trigger"`
	Conf            struct{}  `json:"conf"`
}

type DagRunRequest struct {
	OrderBy          string   `json:"order_by"`
	PageOffset       int      `json:"page_offset"`
	PageLimit        int      `json:"page_limit"`
	DagIds           []string `json:"dag_ids"`
	ExecutionDateGte string   `json:"execution_date_gte,omitempty"`
	ExecutionDateLte string   `json:"execution_date_lte,omitempty"`
}

type airflowClient struct {
	client HTTPClient
}

// TODO : remove project spec
func (ac airflowClient) invoke(ctx context.Context, r airflowRequest, projectSpec models.ProjectSpec) ([]byte, error) {
	var resp []byte
	host, authToken, err := ac.getHostAuth(projectSpec)
	if err != nil {
		return resp, err
	}
	request, err := http.NewRequestWithContext(ctx, r.method, ac.buildEndPoint(host, r.URL, r.param), bytes.NewBuffer(r.body))
	if err != nil {
		return resp, fmt.Errorf("failed to build http request for %s due to %w", r.URL, err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(authToken))))

	HTTPResp, respErr := ac.client.Do(request)
	if respErr != nil {
		return resp, fmt.Errorf("failed to call airflow %s due to %w", r.URL, respErr)
	}
	if HTTPResp.StatusCode != http.StatusOK {
		HTTPResp.Body.Close()
		return resp, fmt.Errorf("status code received %d on calling %s", HTTPResp.StatusCode, r.URL)
	}
	return ac.parseResponse(HTTPResp)
}

func (ac airflowClient) getHostAuth(projectSpec models.ProjectSpec) (string, string, error) {
	schdHost, ok := projectSpec.Config[models.ProjectSchedulerHost]
	if !ok {
		return "", "", fmt.Errorf("scheduler host not set for %s", projectSpec.Name)
	}
	authToken, ok := projectSpec.Secret.GetByName(models.ProjectSchedulerAuth)
	if !ok {
		return "", "", fmt.Errorf("%s secret not configured for project %s", models.ProjectSchedulerAuth, projectSpec.Name)
	}
	schdHost = strings.ReplaceAll(schdHost, "http://", "")
	return schdHost, authToken, nil
}

func (ac airflowClient) parseResponse(resp *http.Response) ([]byte, error) {
	var body []byte
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return body, fmt.Errorf("failed to read airflow response: %w", err)
	}
	return body, nil
}

func (ac airflowClient) buildEndPoint(host, URL, pathParam string) string {
	host = strings.Trim(host, "/")
	u := &url.URL{
		Scheme: "http",
		Host:   host,
	}
	if pathParam != "" {
		u.Path = "/" + strings.ReplaceAll(URL, "%s", pathParam)
	} else {
		u.Path = "/" + URL
	}
	if URL == dagStatusURL {
		params := url.Values{}
		params.Add("limit", "99999")
		u.RawQuery = params.Encode()
	}
	return u.String()
}

func toJobStatus(list DagRunListResponse) ([]models.JobStatus, error) {
	var jobStatus []models.JobStatus
	for _, status := range list.DagRuns {
		jobStatus = append(jobStatus, models.JobStatus{
			ScheduledAt: status.ExecutionDate,
			State:       models.JobRunState(status.State),
		})
	}
	return jobStatus, nil
}

func getDagRunRequest(param *models.JobQuery) DagRunRequest {
	if param.OnlyLastRun {
		return DagRunRequest{
			OrderBy:    "-execution_date",
			PageOffset: 0,
			PageLimit:  1,
			DagIds:     []string{param.Name},
		}
	}
	return DagRunRequest{
		OrderBy:          "execution_date",
		PageOffset:       0,
		PageLimit:        pageLimit,
		DagIds:           []string{param.Name},
		ExecutionDateGte: param.StartDate.Format(airflowDateFormat),
		ExecutionDateLte: param.EndDate.Format(airflowDateFormat),
	}
}

func getJobRuns(res DagRunListResponse, spec *cron.ScheduleSpec) ([]models.JobRun, error) {
	var jobRunList []models.JobRun
	if res.TotalEntries > pageLimit {
		return jobRunList, errors.New("total number of entries exceed page limit")
	}
	for _, dag := range res.DagRuns {
		if !dag.ExternalTrigger {
			var jobRun models.JobRun
			// schedule run
			jobRun = models.JobRun{
				Status:      models.JobRunState(dag.State),
				ScheduledAt: spec.Next(dag.ExecutionDate),
			}
			jobRunList = append(jobRunList, jobRun)
		}
	}
	return jobRunList, nil
}
