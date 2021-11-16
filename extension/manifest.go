package extension

import (
	"fmt"
	"io/ioutil"
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
		content, err := ioutil.ReadFile(manifestPath)
		if err != nil {
			return nil, fmt.Errorf("error reading file: %v", err)
		}
		err = yaml.Unmarshal(content, manifest)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling content: %v", err)
		}
	}
	return manifest, nil
}

// FlushManifest flushes manifest into a file in local machine
func FlushManifest(manifest *Manifest, dirPath string) error {
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
