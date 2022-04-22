package exd

import (
	"fmt"
	"os"
	"path"

	"gopkg.in/yaml.v3"
)

const manifestFileName = "manifest.yaml"

type DefaultManifester struct {
}

// Load loads manifest from local machine
func (d *DefaultManifester) Load(dirPath string) (*Manifest, error) {
	manifestPath := path.Join(dirPath, manifestFileName)
	manifest := &Manifest{}
	if _, err := os.Stat(manifestPath); err == nil {
		content, err := os.ReadFile(manifestPath)
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
func (d *DefaultManifester) Flush(manifest *Manifest, dirPath string) error {
	content, err := yaml.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("error marshalling manifest: %v", err)
	}
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return fmt.Errorf("error creating dir: %v", err)
	}
	manifestPath := path.Join(dirPath, manifestFileName)
	f, err := os.OpenFile(manifestPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer f.Close()

	_, err = f.Write(content)
	if err != nil {
		return fmt.Errorf("error writing file: %v", err)
	}
	return nil
}
