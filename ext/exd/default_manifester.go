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

var DefaultManifesterFS = afero.NewOsFs()

type defaultManifester struct {
}

func NewDefaultManifester() Manifester {
	return &defaultManifester{}
}

// Load loads manifest from local machine
func (d *defaultManifester) Load(dirPath string) (*Manifest, error) {
	manifestPath := path.Join(dirPath, manifestFileName)
	manifest := &Manifest{}
	if _, err := DefaultManifesterFS.Stat(manifestPath); err == nil {
		f, err := DefaultManifesterFS.OpenFile(manifestPath, os.O_RDONLY, 0o755)
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
func (d *defaultManifester) Flush(manifest *Manifest, dirPath string) error {
	content, err := yaml.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("error marshalling manifest: %v", err)
	}
	if err := DefaultManifesterFS.MkdirAll(dirPath, os.ModePerm); err != nil {
		return fmt.Errorf("error creating dir: %v", err)
	}
	manifestPath := path.Join(dirPath, manifestFileName)
	f, err := DefaultManifesterFS.OpenFile(manifestPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer f.Close()
	_, err = f.Write(content)
	return err
}
