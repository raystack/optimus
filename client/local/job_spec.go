package local

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
)

type jobSpecReadWriter struct {
	referenceParentFileName string
	referenceSpecFileName   string
	referenceAssetDirName   string

	specFS afero.Fs
}

func NewJobSpecReadWriter(specFS afero.Fs) (SpecReadWriter[*JobSpec], error) {
	if specFS == nil {
		return nil, errors.New("specFS is nil")
	}
	return &jobSpecReadWriter{
		referenceParentFileName: "this.yaml",
		referenceSpecFileName:   "job.yaml",
		referenceAssetDirName:   "assets",
		specFS:                  specFS,
	}, nil
}

func (j jobSpecReadWriter) ReadAll(rootDirPath string) ([]*JobSpec, error) {
	if rootDirPath == "" {
		return nil, errors.New("root dir path is empty")
	}

	parentsMappedByDirPath, err := j.readParentsMappedByDirPath(rootDirPath)
	if err != nil {
		return nil, fmt.Errorf("error reading parent specs under [%s]: %w", rootDirPath, err)
	}

	dirPaths, err := discoverSpecDirPaths(j.specFS, rootDirPath, j.referenceSpecFileName)
	if err != nil {
		return nil, err
	}
	jobSpecs := make([]*JobSpec, len(dirPaths))
	for i, dirPath := range dirPaths {
		jobSpec, err := j.readCompleteSpec(dirPath)
		if err != nil {
			return nil, err
		}
		j.mergeSpecWithParent(jobSpec, dirPath, parentsMappedByDirPath)
		jobSpecs[i] = jobSpec
	}
	return jobSpecs, nil
}

func (j jobSpecReadWriter) Write(dirPath string, spec *JobSpec) error {
	if dirPath == "" {
		return errors.New("dir path is empty")
	}
	if spec == nil {
		return errors.New("job spec is nil")
	}

	specFilePath := filepath.Join(dirPath, j.referenceSpecFileName)
	if err := writeSpec(j.specFS, specFilePath, spec); err != nil {
		return fmt.Errorf("error writing spec into [%s]: %w", specFilePath, err)
	}
	for assetFileName, assetContent := range spec.Asset {
		assetFilePath := filepath.Join(dirPath, j.referenceAssetDirName, assetFileName)
		if err := j.writeAsset(assetFilePath, assetContent); err != nil {
			return fmt.Errorf("error writing asset into [%s]: %w", assetFilePath, err)
		}
	}
	return nil
}

func (j jobSpecReadWriter) writeAsset(filePath, content string) error {
	f, err := j.specFS.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating asset file into [%s]: %w", filePath, err)
	}
	defer f.Close()
	_, err = f.WriteString(content)
	return err
}

func (jobSpecReadWriter) mergeSpecWithParent(spec *JobSpec, specDirPath string, parentsMappedByDirPath map[string]*JobSpec) {
	splitDirPaths := strings.Split(specDirPath, "/")
	for i := range splitDirPaths {
		pathNearSpecIdx := len(splitDirPaths) - i
		rootToNearSpecPaths := splitDirPaths[:pathNearSpecIdx]
		parentDirPath := filepath.Join(rootToNearSpecPaths...)
		if parentJobSpec, ok := parentsMappedByDirPath[parentDirPath]; ok {
			spec.MergeFrom(*parentJobSpec)
		}
	}
}

func (j jobSpecReadWriter) readParentsMappedByDirPath(rootDirPath string) (map[string]*JobSpec, error) {
	parentDirPaths, err := discoverSpecDirPaths(j.specFS, rootDirPath, j.referenceParentFileName)
	if err != nil {
		return nil, fmt.Errorf("error discovering parent spec paths under [%s]: %w", rootDirPath, err)
	}
	parentsMappedByDirPath := make(map[string]*JobSpec)
	for _, dirPath := range parentDirPaths {
		filePath := filepath.Join(dirPath, j.referenceParentFileName)
		spec, err := readSpec[*JobSpec](j.specFS, filePath)
		if err != nil {
			return nil, fmt.Errorf("error reading spec under [%s]: %w", filePath, err)
		}
		parentsMappedByDirPath[dirPath] = spec
	}
	return parentsMappedByDirPath, nil
}

func (j jobSpecReadWriter) readCompleteSpec(dirPath string) (*JobSpec, error) {
	specFilePath := filepath.Join(dirPath, j.referenceSpecFileName)
	jobSpec, err := readSpec[*JobSpec](j.specFS, specFilePath)
	if err != nil {
		return nil, fmt.Errorf("error reading spec under [%s]: %w", dirPath, err)
	}
	assetsMappedByFileName, err := j.readAssetsMappedByFileName(dirPath)
	if err != nil {
		return nil, fmt.Errorf("error reading asset under [%s]: %w", dirPath, err)
	}
	jobSpec.Asset = assetsMappedByFileName
	return jobSpec, nil
}

func (j jobSpecReadWriter) readAssetsMappedByFileName(dirPath string) (map[string]string, error) {
	assetDirPath := filepath.Join(dirPath, j.referenceAssetDirName)
	assetFilePaths, err := discoverFilePaths(j.specFS, assetDirPath)
	if err != nil {
		return nil, fmt.Errorf("error discovering asset file paths under [%s]: %w", assetDirPath, err)
	}

	assetsMap := make(map[string]string)
	for _, assetFilePath := range assetFilePaths {
		assetContent, err := j.readAssetFile(assetFilePath)
		if err != nil {
			return nil, fmt.Errorf("error reading asset file under [%s]: %w", assetFilePath, err)
		}
		assetFileName := strings.TrimPrefix(assetFilePath, dirPath)
		assetFileName = strings.TrimPrefix(assetFileName, "/")
		assetsMap[assetFileName] = string(assetContent)
	}
	return assetsMap, nil
}

func (j jobSpecReadWriter) readAssetFile(assetFilePath string) ([]byte, error) {
	f, err := j.specFS.Open(assetFilePath)
	if err != nil {
		return nil, fmt.Errorf("error opening asset file under [%s]: %w", assetFilePath, err)
	}
	defer f.Close()

	return io.ReadAll(f)
}
