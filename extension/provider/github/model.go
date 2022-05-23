package github

import (
	"fmt"
	"regexp"

	"github.com/odpf/optimus/extension/model"
)

const apiPrefix = "https://api.github.com/repos"

// Release defines github repository release
type Release struct {
	TagName    string   `json:"tag_name"`
	Draft      bool     `json:"draft"`
	Prerelease bool     `json:"prerelease"`
	Assets     []*Asset `json:"assets"`
}

func (r *Release) toRepositoryRelease(apiPath string) *model.RepositoryRelease {
	assets := make([]*model.RepositoryAsset, len(r.Assets))
	for i, a := range r.Assets {
		assets[i] = a.toRepositoryAsset()
	}
	return &model.RepositoryRelease{
		TagName: r.TagName,
		Metadata: map[string]interface{}{
			"draft":      r.Draft,
			"prerelease": r.Prerelease,
		},
		UpgradeAPIPath: r.getUpgradeAPIPath(apiPath),
		CurrentAPIPath: r.getCurrentAPIPath(apiPath),
		Assets:         assets,
	}
}

func (r *Release) getCurrentAPIPath(apiPath string) string {
	detectLatest := regexp.MustCompile(`latest/?$`)
	if found := detectLatest.FindString(apiPath); found != "" {
		repl := fmt.Sprintf("tags/%s", r.TagName)
		apiPath = detectLatest.ReplaceAllString(apiPath, repl)
	}
	return apiPath
}

func (*Release) getUpgradeAPIPath(apiPath string) string {
	detectTag := regexp.MustCompile(`tags/\S+`)
	if found := detectTag.FindString(apiPath); found != "" {
		apiPath = detectTag.ReplaceAllString(apiPath, "latest")
	}
	return apiPath
}

// Asset defines github release asset
type Asset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

func (a *Asset) toRepositoryAsset() *model.RepositoryAsset {
	return &model.RepositoryAsset{
		Name: a.Name,
		URL:  a.BrowserDownloadURL,
	}
}
