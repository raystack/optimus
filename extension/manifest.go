package extension

import (
	"fmt"
	"os"
	"path"
	"time"

	"gopkg.in/yaml.v3"
)

const manifestFileName = "manifest.yaml"

// Manifest contains information on the extension in local machine
type Manifest struct {
	Update    time.Time   `yaml:"update"`
	Metadatas []*Metadata `yaml:"metadatas"`
}

// Metadata contains metadata for a particular extension
type Metadata struct {
	Owner     string   `yaml:"owner"`
	Repo      string   `yaml:"repo"`
	Aliases   []string `yaml:"aliases"`
	Tag       string   `yaml:"tag"`
	LocalPath string   `yaml:"local_path"`
}

// LoadManifest loads manifest from local machine
func LoadManifest(dirPath string) (*Manifest, error) {
	manifestPath := path.Join(dirPath, manifestFileName)
	manifest := &Manifest{}
	if _, err := os.Stat(manifestPath); err == nil {
		content, err := os.ReadFile(manifestPath)
		if err != nil {
			return nil, fmt.Errorf("error reading file: %w", err)
		}
		err = yaml.Unmarshal(content, manifest)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling content: %w", err)
		}
	}
	return manifest, nil
}

// FlushManifest flushes manifest into a file in local machine
func FlushManifest(manifest *Manifest, dirPath string) error {
	content, err := yaml.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("error marshalling manifest: %w", err)
	}
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil { // TODO: Dangerous 0777 permissions
		return fmt.Errorf("error creating dir: %w", err)
	}
	manifestPath := path.Join(dirPath, manifestFileName)
	f, err := os.OpenFile(manifestPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()

	_, err = f.Write(content)
	if err != nil {
		return fmt.Errorf("error writing file: %w", err)
	}
	return nil
}
