package airflow2

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

	"github.com/odpf/optimus/models"
)

const (
	Hundred = 100
)

type airflowRequest struct {
	URL    string
	method string
	body   []byte
	param  string
}

type DagRunList struct {
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

type DagRunReqBody struct {
	OrderBy          string   `json:"order_by"`
	PageOffset       int      `json:"page_offset"`
	PageLimit        int      `json:"page_limit"`
	DagIds           []string `json:"dag_ids"`
	ExecutionDateGte string   `json:"execution_date_gte"`
	ExecutionDateLte string   `json:"execution_date_lte"`
}

type airflowClient struct {
	client HTTPClient
}

func newClient(client HTTPClient) airflowClient {
	return airflowClient{client: client}
}

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
	HTTPResp, err := ac.client.Do(request)
	if err != nil {
		return resp, fmt.Errorf("failed to call airflow %s due to %w", r.URL, err)
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

func (ac airflowClient) buildEndPoint(host string, URL string, pathParam string) string {
	host = strings.Trim(host, "/")
	url := &url.URL{
		Scheme: "http",
		Host:   host,
	}
	if pathParam != "" {
		url.Path = "/" + strings.ReplaceAll(URL, "%s", pathParam)
	} else {
		url.Path = "/" + URL
	}
	return url.String()
}

func toJobStatus(list DagRunList) ([]models.JobStatus, error) {
	var jobStatus []models.JobStatus
	for _, status := range list.DagRuns {
		jobStatus = append(jobStatus, models.JobStatus{
			ScheduledAt: status.ExecutionDate,
			State:       models.JobRunState(status.State),
		})
	}
	return jobStatus, nil
}

func getDagRunReqBody(param models.JobQuery) DagRunReqBody {
	if param.OnlyLastRun {
		return DagRunReqBody{
			OrderBy:    "-execution_date",
			PageOffset: 0,
			PageLimit:  1,
			DagIds:     []string{param.Name},
		}
	}
	return DagRunReqBody{
		OrderBy:          "execution_date",
		PageOffset:       0,
		PageLimit:        Hundred,
		DagIds:           []string{param.Name},
		ExecutionDateGte: param.StartDate.Format(airflowDateFormat),
		ExecutionDateLte: param.EndDate.Format(airflowDateFormat),
	}
}

func getJobRunList(res DagRunList) []models.JobRun {
	var jobRunList []models.JobRun
	for _, dag := range res.DagRuns {
		if !dag.ExternalTrigger {
			jobRun := models.JobRun{
				Status:      models.JobRunState(dag.State),
				ScheduledAt: dag.ExecutionDate,
				ExecutedAt:  dag.StartDate,
			}
			jobRunList = append(jobRunList, jobRun)
		}
	}
	return jobRunList
}
