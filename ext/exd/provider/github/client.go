package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"

	"github.com/odpf/optimus/ext/exd"
)

const providerName = "github"

// RepositoryRelease defines github repository release
type RepositoryRelease struct {
	TagName    string          `json:"tag_name"`
	Draft      bool            `json:"draft"`
	Prerelease bool            `json:"prerelease"`
	Assets     []*ReleaseAsset `json:"assets"`
}

// ReleaseAsset defines github release asset
type ReleaseAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

// Client defines github client
type Client struct {
	ctx      context.Context
	httpdoer exd.HTTPDoer
}

// NewClient initializes github client
func NewClient(ctx context.Context, httpDoer exd.HTTPDoer) (*Client, error) {
	if ctx == nil {
		return nil, exd.ErrNilContext
	}
	if httpDoer == nil {
		return nil, exd.ErrNilHTTPDoer
	}
	return &Client{
		ctx:      ctx,
		httpdoer: httpDoer,
	}, nil
}

// Download downloads github asset based on the metadata
func (c *Client) Download(metadata *exd.Metadata) ([]byte, error) {
	if metadata == nil {
		return nil, exd.ErrNilMetadata
	}
	if metadata.ProviderName != providerName {
		return nil, fmt.Errorf("metadata provider is not %s", providerName)
	}
	repositoryRelease, err := c.getRepositoryRelease(metadata.AssetAPIPath)
	if err != nil {
		return nil, fmt.Errorf("error getting repository release: %w", err)
	}
	assetURL, err := c.getAssetURL(repositoryRelease)
	if err != nil {
		return nil, fmt.Errorf("error getting asset URL: %w", err)
	}
	return c.downloadAsset(assetURL)
}

func (c *Client) downloadAsset(url string) ([]byte, error) {
	request, err := http.NewRequestWithContext(c.ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	request.Header.Set("Accept", "application/octet-stream")

	response, err := c.httpdoer.Do(request)
	if err != nil {
		return nil, fmt.Errorf("error getting release asset: %w", err)
	}
	defer response.Body.Close()

	return io.ReadAll(response.Body)
}

func (c *Client) getAssetURL(release *RepositoryRelease) (string, error) {
	suffix := c.getDistSuffix()
	for _, asset := range release.Assets {
		if strings.HasSuffix(asset.Name, suffix) {
			return asset.BrowserDownloadURL, nil
		}
	}
	return "", fmt.Errorf("asset with suffix [%s] is not found", suffix)
}

func (c *Client) getDistSuffix() string {
	return runtime.GOOS + "-" + runtime.GOARCH
}

func (c *Client) getRepositoryRelease(apitPath string) (*RepositoryRelease, error) {
	request, err := http.NewRequestWithContext(c.ctx, "GET", apitPath, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	request.Header.Set("Accept", "application/json")

	response, err := c.httpdoer.Do(request)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %w", err)
	}
	defer response.Body.Close()

	var repositoryRelease RepositoryRelease
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&repositoryRelease); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	if repositoryRelease.Draft || repositoryRelease.Prerelease {
		return nil, errors.New("specified release tag is either a draft or a pre-release")
	}
	return &repositoryRelease, nil
}

func init() {
	if err := exd.NewClientRegistry.Add(providerName,
		func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return NewClient(ctx, httpDoer)
		},
	); err != nil {
		panic(err)
	}
}
