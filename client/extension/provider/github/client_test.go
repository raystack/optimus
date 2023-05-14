package github_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/client/extension/provider/github"
)

type ClientTestSuite struct {
	suite.Suite
}

func (c *ClientTestSuite) TestDownloadRelease() {
	ctx := context.Background()
	client := &github.Client{}

	c.Run("should return nil and error if asset api path is empty", func() {
		var apiPath string

		actualRelease, actualErr := client.DownloadRelease(ctx, apiPath)

		c.Nil(actualRelease)
		c.Error(actualErr)
	})

	c.Run("should return nil and error if error when creating request to API path", func() {
		apiPath := ":invalid-url"

		actualRelease, actualErr := client.DownloadRelease(ctx, apiPath)

		c.Nil(actualRelease)
		c.Error(actualErr)
	})

	c.Run("should return nil and error if encountered error when doing request", func() {
		apiPath := "/gojek/optimus-extension-valor"

		actualRelease, actualErr := client.DownloadRelease(ctx, apiPath)

		c.Nil(actualRelease)
		c.Error(actualErr)
	})

	c.Run("should return nil and error if response status is not ok", func() {
		testPath := "/gojek/optimus-extension-valor"

		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
		})
		server := httptest.NewServer(handler)
		defer server.Close()

		apiPath := server.URL + testPath
		actualRelease, actualErr := client.DownloadRelease(ctx, apiPath)

		c.Nil(actualRelease)
		c.Error(actualErr)
	})

	c.Run("should return nil and error if encountered error when decoding response", func() {
		testPath := "/gojek/optimus-extension-valor"
		message := "invalid-content"

		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			content := []byte(message)

			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(content)
		})
		server := httptest.NewServer(handler)
		defer server.Close()

		apiPath := server.URL + testPath
		actualRelease, actualErr := client.DownloadRelease(ctx, apiPath)

		c.Nil(actualRelease)
		c.Error(actualErr)
	})

	c.Run("should return release and nil if no error is encountered", func() {
		testPath := "/gojek/optimus-extension-valor"
		release := &github.Release{}

		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			content, _ := json.Marshal(release)

			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(content)
		})
		server := httptest.NewServer(handler)
		defer server.Close()

		apiPath := server.URL + testPath
		actualRelease, actualErr := client.DownloadRelease(ctx, apiPath)

		c.NotNil(actualRelease)
		c.NoError(actualErr)
	})
}

func (c *ClientTestSuite) TestDownloadAsset() {
	ctx := context.Background()
	client := &github.Client{}

	c.Run("should return nil and error if asset api path is empty", func() {
		var apiPath string

		actualAsset, actualErr := client.DownloadAsset(ctx, apiPath)

		c.Nil(actualAsset)
		c.Error(actualErr)
	})

	c.Run("should return nil and error if encountered error when getting release", func() {
		testReleasePath := "/gojek/optimus-extension-valor"
		message := "invalid-content"

		router := http.NewServeMux()
		server := httptest.NewServer(router)
		defer server.Close()

		router.HandleFunc(testReleasePath, func(w http.ResponseWriter, r *http.Request) {
			content := []byte(message)

			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(content)
		})

		actualRelease, actualErr := client.DownloadAsset(ctx, testReleasePath)

		c.Nil(actualRelease)
		c.Error(actualErr)
	})

	c.Run("should return nil and error if cannot find asset with the specified suffix", func() {
		testReleasePath := "/gojek/optimus-extension-valor"
		release := &github.Release{}

		router := http.NewServeMux()
		server := httptest.NewServer(router)
		defer server.Close()

		releaseAPIPath := server.URL + testReleasePath
		router.HandleFunc(testReleasePath, func(w http.ResponseWriter, r *http.Request) {
			content, _ := json.Marshal(release)

			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(content)
		})

		actualAsset, actualErr := client.DownloadAsset(ctx, releaseAPIPath)

		c.Nil(actualAsset)
		c.Error(actualErr)
	})

	c.Run("should return nil and error if error when creating request to download asset", func() {
		testReleasePath := "/gojek/optimus-extension-valor"
		release := &github.Release{
			Assets: []*github.Asset{
				{
					Name:               "asset" + runtime.GOOS + "-" + runtime.GOARCH,
					BrowserDownloadURL: ":invalid-url",
				},
			},
		}

		router := http.NewServeMux()
		server := httptest.NewServer(router)
		defer server.Close()

		releaseAPIPath := server.URL + testReleasePath
		router.HandleFunc(testReleasePath, func(w http.ResponseWriter, r *http.Request) {
			content, _ := json.Marshal(release)

			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(content)
		})

		actualAsset, actualErr := client.DownloadAsset(ctx, releaseAPIPath)

		c.Nil(actualAsset)
		c.Error(actualErr)
	})

	c.Run("should return nil and error if download asset status is not ok", func() {
		testReleasePath := "/gojek/optimus-extension-valor"
		release := &github.Release{
			Assets: []*github.Asset{
				{
					Name:               "asset" + runtime.GOOS + "-" + runtime.GOARCH,
					BrowserDownloadURL: "/optimus/releases",
				},
			},
		}

		router := http.NewServeMux()
		server := httptest.NewServer(router)
		defer server.Close()

		releaseAPIPath := server.URL + testReleasePath
		router.HandleFunc(testReleasePath, func(w http.ResponseWriter, r *http.Request) {
			content, _ := json.Marshal(release)

			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(content)
		})
		testAssetPath := release.Assets[0].BrowserDownloadURL
		assetAPIPath := server.URL + testAssetPath
		release.Assets[0].BrowserDownloadURL = assetAPIPath
		router.HandleFunc(testAssetPath, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
		})

		actualAsset, actualErr := client.DownloadAsset(ctx, releaseAPIPath)

		c.Nil(actualAsset)
		c.Error(actualErr)
	})

	c.Run("should return bytes and nil if no error is encountered", func() {
		testReleasePath := "/gojek/optimus-extension-valor"
		release := &github.Release{
			Assets: []*github.Asset{
				{
					Name:               "asset" + runtime.GOOS + "-" + runtime.GOARCH,
					BrowserDownloadURL: "/optimus/releases",
				},
			},
		}
		assetPayload := "valid random payload"

		router := http.NewServeMux()
		server := httptest.NewServer(router)
		defer server.Close()

		releaseAPIPath := server.URL + testReleasePath
		router.HandleFunc(testReleasePath, func(w http.ResponseWriter, r *http.Request) {
			content, _ := json.Marshal(release)

			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(content)
		})
		testAssetPath := release.Assets[0].BrowserDownloadURL
		assetAPIPath := server.URL + testAssetPath
		release.Assets[0].BrowserDownloadURL = assetAPIPath
		router.HandleFunc(testAssetPath, func(w http.ResponseWriter, r *http.Request) {
			content := []byte(assetPayload)

			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(content)
		})

		actualAsset, actualErr := client.DownloadAsset(ctx, releaseAPIPath)

		c.NotNil(actualAsset)
		c.NoError(actualErr)
	})
}

func TestGithub(t *testing.T) {
	suite.Run(t, &ClientTestSuite{})
}
