package local

import (
	"errors"
	"io"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
)

type jobSpecReadWriter struct {
	referenceJobSpecFileName       string
	referenceParentJobSpecFileName string
	referenceAssetDirName          string
	specFS                         afero.Fs
}

func NewJobSpecReadWriter(specFS afero.Fs) (SpecReadWriter[*JobSpec], error) {
	if specFS == nil {
		return nil, errors.New("specFS is nil")
	}

	return &jobSpecReadWriter{
		referenceJobSpecFileName:       "job.yaml",
		referenceParentJobSpecFileName: "this.yaml",
		referenceAssetDirName:          "assets",
		specFS:                         specFS,
	}, nil
}

func (j jobSpecReadWriter) ReadAll(rootDirPath string) ([]*JobSpec, error) {
	dirPaths, err := discoverSpecDirPaths(j.specFS, rootDirPath, j.referenceJobSpecFileName)
	if err != nil {
		return nil, err
	}
	dirParentPaths, err := discoverSpecDirPaths(j.specFS, rootDirPath, j.referenceParentJobSpecFileName)
	if err != nil {
		return nil, err
	}

	// read all spec parents (this.yaml)
	parentJobSpecsMap := map[string]*JobSpec{}
	for _, dirPath := range dirParentPaths {
		filePath := filepath.Join(dirPath, j.referenceParentJobSpecFileName)
		parentJobSpec, err := readJobSpecFromFilePath(j.specFS, filePath)
		if err != nil {
			return nil, err
		}
		parentJobSpecsMap[dirPath] = parentJobSpec
	}

	// read job specs
	var jobSpecs []*JobSpec
	for _, dirPath := range dirPaths {
		jobSpec, err := j.read(dirPath)
		if err != nil {
			return nil, err
		}

		// merge with parent job specs
		splittedPath := strings.Split(dirPath, "/")
		for i := range splittedPath {
			currentDirPath := strings.Join(splittedPath[:len(splittedPath)-i], "/")
			if parentJobSpec, ok := parentJobSpecsMap[currentDirPath]; ok {
				jobSpec.MergeFrom(*parentJobSpec)
			}
		}
		jobSpecs = append(jobSpecs, jobSpec)
	}

	return jobSpecs, nil
}

func (j jobSpecReadWriter) Write(dirPath string, spec *JobSpec) error {
	if spec == nil {
		return errors.New("job spec is nil")
	}

	// write job spec
	jobSpecFilePath := filepath.Join(dirPath, j.referenceJobSpecFileName)
	if err := writeJobSpecToFilePath(j.specFS, jobSpecFilePath, spec); err != nil {
		return err
	}

	// write assets
	for assetFileName, assetContent := range spec.Asset {
		assetFilePath := filepath.Join(dirPath, j.referenceAssetDirName, assetFileName)
		if err := writeAssetToFilePath(j.specFS, assetFilePath, assetContent); err != nil {
			return err
		}
	}

	return nil
}

func (j jobSpecReadWriter) read(dirPath string) (*JobSpec, error) {
	// read job.yaml
	specFilePath := filepath.Join(dirPath, j.referenceJobSpecFileName)
	jobSpec, err := readJobSpecFromFilePath(j.specFS, specFilePath)
	if err != nil {
		return nil, err
	}

	// read assets
	assetDirPath := filepath.Join(dirPath, j.referenceAssetDirName)
	assets, err := readAssetsFromDirPath(j.specFS, assetDirPath)
	if err != nil {
		return nil, err
	}

	// construct
	jobSpec.Asset = assets

	return jobSpec, nil
}

func readJobSpecFromFilePath(fileFS afero.Fs, filePath string) (*JobSpec, error) {
	fileSpec, err := fileFS.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer fileSpec.Close()

	var jobSpec JobSpec
	if err := yaml.NewDecoder(fileSpec).Decode(&jobSpec); err != nil {
		return nil, err
	}

	return &jobSpec, nil
}

func readAssetsFromDirPath(fileFS afero.Fs, dirPath string) (map[string]string, error) {
	assetFilePaths, err := discoverFilePaths(fileFS, dirPath)
	if err != nil {
		return nil, err
	}

	assetsMap := make(map[string]string)
	for _, assetFilePath := range assetFilePaths {
		assetContent, err := readAssetFromFilePath(fileFS, assetFilePath)
		if err != nil {
			return nil, err
		}

		assetFileName := strings.TrimPrefix(assetFilePath, dirPath)
		assetFileName = strings.TrimPrefix(assetFileName, "/")
		assetsMap[assetFileName] = string(assetContent)
	}

	return assetsMap, nil
}

func readAssetFromFilePath(fileFS afero.Fs, filePath string) ([]byte, error) {
	f, err := fileFS.Open(filePath)
	if err != nil {
		return nil, err
	}

	rawAssetContent, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return rawAssetContent, nil
}

func writeJobSpecToFilePath(fileFS afero.Fs, filePath string, jobSpec *JobSpec) error {
	f, err := fileFS.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	jobSpecRaw, err := yaml.Marshal(jobSpec)
	if err != nil {
		return err
	}

	_, err = f.Write(jobSpecRaw)
	return err
}

func writeAssetToFilePath(fileFS afero.Fs, filePath string, content string) error {
	f, err := fileFS.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(content)
	return err
}
