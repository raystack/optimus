package extension

import (
	"context"
	"net/http"

	"github.com/google/go-github/github"
)

// GithubReleaseGetter gets the latest Github release
type GithubReleaseGetter interface {
	GetLatestRelease(ctx context.Context, owner, repo string) (*github.RepositoryRelease, *github.Response, error)
}

// HTTPDoer is an HTTP contract to do a request
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}
