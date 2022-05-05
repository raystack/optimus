package exd

import (
	"fmt"
	"io"
	"os"
	"path"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

const manifestFileName = "manifest.yaml"

// ManifesterFS is file system that will be used for manifester operations.
// It can be changed before calling any manifester operation.
// But, make sure to change it back after the operation is done
// to its default value to avoid unexpected behaviour.
var ManifesterFS = afero.NewOsFs()

type defaultManifester struct {
}

// NewDefaultManifester initializes default manifester
func NewDefaultManifester() Manifester {
	return &defaultManifester{}
}

// Load loads manifest from local machine
func (*defaultManifester) Load(dirPath string) (*Manifest, error) {
	manifestPath := path.Join(dirPath, manifestFileName)
	manifest := &Manifest{}
	if _, err := ManifesterFS.Stat(manifestPath); err == nil {
		f, err := ManifesterFS.OpenFile(manifestPath, os.O_RDONLY, 0o755)
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
	}
	return manifest, nil
}

// Flush flushes manifest into a file in local machine
func (*defaultManifester) Flush(manifest *Manifest, dirPath string) error {
	if manifest == nil {
		return ErrNilManifester
	}
	content, err := yaml.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("error marshalling manifest: %w", err)
	}
	if err := ManifesterFS.MkdirAll(dirPath, 0o755); err != nil {
		return fmt.Errorf("error creating dir: %w", err)
	}
	manifestPath := path.Join(dirPath, manifestFileName)
	f, err := ManifesterFS.OpenFile(manifestPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()
	_, err = f.Write(content)
	return err
}
