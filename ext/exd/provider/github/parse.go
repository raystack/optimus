package github

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/odpf/optimus/ext/exd"
)

// Parse parses remote path to get its metadata according to github convention
func Parse(remotePath string) (*exd.RemoteMetadata, error) {
	if err := validate(remotePath); err != nil {
		return nil, fmt.Errorf("error validating remote path: %w", err)
	}

	cleanedRemotePath := removeURLPrefix(remotePath)
	ownerName := extractOwner(cleanedRemotePath)
	repoName := extractRepoName(cleanedRemotePath)
	tagName := extractTag(cleanedRemotePath)

	return &exd.RemoteMetadata{
		ProviderName: provider,
		OwnerName:    ownerName,
		RepoName:     repoName,
		TagName:      tagName,
		AssetAPIPath: composeAPIPath(ownerName, repoName, tagName),
		AssetDirPath: composeAssetDirPath(ownerName, repoName),
		CommandName:  extractCommandName(repoName),
	}, nil
}

func extractCommandName(repoName string) string {
	loweredRepoName := strings.ToLower(repoName)
	return strings.Replace(loweredRepoName, "optimus-extension-", "", 1)
}

func composeAssetDirPath(ownerName, repoName string) string {
	hostName := provider + ".com"
	return path.Join(exd.ExtensionDir, hostName, ownerName, repoName)
}

func composeAPIPath(ownerName, repoName, tagName string) string {
	prefix := "https://api.github.com/repos"
	output := fmt.Sprintf("%s/%s/%s/releases", prefix, ownerName, repoName)
	if tagName == "" || tagName == "latest" {
		output = fmt.Sprintf("%s/latest", output)
	} else {
		output = fmt.Sprintf("%s/tags/%s", output, tagName)
	}
	return output
}

func extractTag(cleanedRemotePath string) string {
	splitPath := strings.Split(cleanedRemotePath, "@")
	var output string
	if len(splitPath) > 1 {
		output = splitPath[1]
	}
	return output
}

func extractRepoName(cleanedRemotePath string) string {
	splitPath := strings.Split(cleanedRemotePath, "/")
	return strings.Split(splitPath[1], "@")[0]
}

func extractOwner(cleanedRemotePath string) string {
	splitPath := strings.Split(cleanedRemotePath, "/")
	return splitPath[0]
}

func removeURLPrefix(remotePath string) string {
	removePrefix := regexp.MustCompile(`^((https?:\/\/)?(www\.)?github\.com/)?`)
	return removePrefix.ReplaceAllString(remotePath, "")
}

func validate(remotePath string) error {
	if remotePath == "" {
		return exd.ErrEmptyRemotePath
	}
	detectGithub := regexp.MustCompile(`^((https?:\/\/)?(www\.)?github\.com/)?([a-zA-Z0-9\-]+/optimus-extension-[a-zA-Z0-9\-]+(@\S+)?)$`)
	if result := detectGithub.FindString(remotePath); result == "" {
		return fmt.Errorf("%s can't recognize remote path: %w", provider, exd.ErrUnrecognizedRemotePath)
	}
	_, err := os.UserHomeDir()
	return err
}

func init() { //nolint:gochecknoinits
	exd.ParseRegistry = append(exd.ParseRegistry, Parse)
}