package github

import "github.com/odpf/optimus/ext/exd"

// Release defines github repository release
type Release struct {
	TagName    string   `json:"tag_name"`
	Draft      bool     `json:"draft"`
	Prerelease bool     `json:"prerelease"`
	Assets     []*Asset `json:"assets"`
}

func (r *Release) toRepositoryRelease() *exd.RepositoryRelease {
	assets := make([]*exd.RepositoryAsset, len(r.Assets))
	for i, a := range r.Assets {
		assets[i] = a.toRepositoryAsset()
	}
	return &exd.RepositoryRelease{
		TagName: r.TagName,
		Metadata: map[string]interface{}{
			"draft":      r.Draft,
			"prerelease": r.Prerelease,
		},
		Assets: assets,
	}
}

// Asset defines github release asset
type Asset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

func (a *Asset) toRepositoryAsset() *exd.RepositoryAsset {
	return &exd.RepositoryAsset{
		Name: a.Name,
		URL:  a.BrowserDownloadURL,
	}
}
