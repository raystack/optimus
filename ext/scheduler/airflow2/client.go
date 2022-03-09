package airflow2

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
)

type airflowRequest struct {
	URL    string
	method string
	token  string
	body   []byte
}

type airflowClient struct {
	client HTTPClient
}

func newClient(client HTTPClient) airflowClient {
	return airflowClient{client: client}
}

func (ac airflowClient) invoke(ctx context.Context, r airflowRequest) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, r.method, r.URL, bytes.NewBuffer(r.body))
	if err != nil {
		return nil, fmt.Errorf("failed to build http request for %s due to %w", r.URL, err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(r.token))))
	resp, err := ac.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("failed to call airflow %s due to %w", r.URL, err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("status code received %d on calling %s", resp.StatusCode, r.URL)
	}
	return resp, nil
}

func buildEndPoint(URL string, host string, pathParam string) string {
	var sb strings.Builder
	host = strings.Trim(host, "/")
	sb.WriteString(host)
	sb.WriteString("/")
	if pathParam != "" {
		sb.WriteString(strings.Replace(URL, "%s", pathParam, -1))
	} else {
		sb.WriteString(URL)
	}
	return sb.String()
}
