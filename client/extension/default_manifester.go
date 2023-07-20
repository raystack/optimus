package extension

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"

	"github.com/raystack/optimus/client/extension/model"
)

const manifestFileName = "manifest.yaml"

// ManifesterFS is file system that will be used for manifester operations.
// It can be changed before calling any manifester operation.
// But, make sure to change it back after the operation is done
// to its default value to avoid unexpected behaviour.
var ManifesterFS = afero.NewOsFs()

type defaultManifester struct{}

// NewDefaultManifester initializes default manifester
func NewDefaultManifester() model.Manifester {
	return &defaultManifester{}
}

// Load loads manifest from local machine
func (d *defaultManifester) Load(dirPath string) (*model.Manifest, error) {
	manifestPath := path.Join(dirPath, manifestFileName)
	manifest := &model.Manifest{}
	if _, err := ManifesterFS.Stat(manifestPath); err == nil {
		filePermission := 0o644
		f, err := ManifesterFS.OpenFile(manifestPath, os.O_RDONLY, fs.FileMode(filePermission))
		if err != nil {
			return nil, fmt.Errorf("error opening manifest file: %w", err)
		}
		defer f.Close()
		content, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("error reading manifest file: %w", err)
		}
		err = yaml.Unmarshal(content, manifest)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling manifest content: %w", err)
		}
		d.enrichManifest(manifest)
	}
	return manifest, nil
}

func (*defaultManifester) enrichManifest(manifest *model.Manifest) {
	for _, owner := range manifest.RepositoryOwners {
		for _, project := range owner.Projects {
			project.Owner = owner
			for _, release := range project.Releases {
				release.Project = project
			}
		}
	}
}

// Flush flushes manifest into a file in local machine
func (*defaultManifester) Flush(manifest *model.Manifest, dirPath string) error {
	if manifest == nil {
		return model.ErrNilManifester
	}
	content, err := yaml.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("error marshalling manifest: %w", err)
	}
	directoryPermission := 0o744
	if err := ManifesterFS.MkdirAll(dirPath, fs.FileMode(directoryPermission)); err != nil {
		return fmt.Errorf("error creating manifest dir: %w", err)
	}
	manifestPath := path.Join(dirPath, manifestFileName)
	filePermission := 0o644
	f, err := ManifesterFS.OpenFile(manifestPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fs.FileMode(filePermission))
	if err != nil {
		return fmt.Errorf("error opening manifest file: %w", err)
	}
	defer f.Close()
	_, err = f.Write(content)
	return err
}
