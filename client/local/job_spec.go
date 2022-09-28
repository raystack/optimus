package local

import (
	"errors"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
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
		parentJobSpec, err := readSpec[*JobSpec](j.specFS, filePath)
		if err != nil {
			return nil, err
		}
		parentJobSpecsMap[dirPath] = parentJobSpec
	}

	// read job specs
	var jobSpecs []*JobSpec
	for _, dirPath := range dirPaths {
		jobSpec, err := j.readJobSpecFromDirPath(dirPath)
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
	if err := writeSpec[*JobSpec](j.specFS, jobSpecFilePath, spec); err != nil {
		return err
	}

	// write assets
	for assetFileName, assetContent := range spec.Asset {
		assetFilePath := filepath.Join(dirPath, j.referenceAssetDirName, assetFileName)
		if err := writeFile(j.specFS, assetFilePath, assetContent); err != nil {
			return err
		}
	}

	return nil
}

func (j jobSpecReadWriter) readJobSpecFromDirPath(dirPath string) (*JobSpec, error) {
	// read job.yaml
	specFilePath := filepath.Join(dirPath, j.referenceJobSpecFileName)
	jobSpec, err := readSpec[*JobSpec](j.specFS, specFilePath)
	if err != nil {
		return nil, err
	}

	// read assets
	assetDirPath := filepath.Join(dirPath, j.referenceAssetDirName)
	assets, err := j.readAssetsFromDirPath(assetDirPath)
	if err != nil {
		return nil, err
	}

	// construct
	jobSpec.Asset = assets

	return jobSpec, nil
}

func (j jobSpecReadWriter) readAssetsFromDirPath(dirPath string) (map[string]string, error) {
	assetFilePaths, err := discoverFilePaths(j.specFS, dirPath)
	if err != nil {
		return nil, err
	}

	assetsMap := make(map[string]string)
	for _, assetFilePath := range assetFilePaths {
		assetContent, err := readFile(j.specFS, assetFilePath)
		if err != nil {
			return nil, err
		}

		assetFileName := strings.TrimPrefix(assetFilePath, dirPath)
		assetFileName = strings.TrimPrefix(assetFileName, "/")
		assetsMap[assetFileName] = string(assetContent)
	}

	return assetsMap, nil
}
