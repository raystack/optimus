package local

import (
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v2"
)

type jobSpecReadWriter struct {
	referenceJobSpecFileName string
	referenceAssetDirName    string
	specFS                   fs.FS
}

func NewJobSpecReadWriter(specFS fs.FS) (SpecReadWriter[*JobSpec], error) {
	if specFS == nil {
		return nil, errors.New("specFS is nil")
	}

	return &jobSpecReadWriter{
		referenceJobSpecFileName: "job.yaml",
		referenceAssetDirName:    "assets",
		specFS:                   specFS,
	}, nil
}

func (j jobSpecReadWriter) ReadAll(rootDirPath string) ([]*JobSpec, error) {
	dirPaths, err := discoverSpecDirPaths(j.specFS, rootDirPath, j.referenceJobSpecFileName)
	if err != nil {
		return nil, err
	}
	var output []*JobSpec
	for _, p := range dirPaths {
		spec, err := j.read(p)
		if err != nil {
			return nil, err
		}
		output = append(output, spec)
	}
	return output, nil
}

func (jobSpecReadWriter) Write(dirPath string, spec *JobSpec) error {
	// TODO: implement write job spec here. Given dirPath and job spec
	// create job.yaml specification as well as their asset inside dirPath folder
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

func readJobSpecFromFilePath(fileFS fs.FS, filePath string) (*JobSpec, error) {
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

func readAssetsFromDirPath(fileFS fs.FS, dirPath string) (map[string]string, error) {
	assetFilePaths, err := discoverAssetFilePaths(fileFS, dirPath)
	if err != nil {
		return nil, err
	}

	assetsMap := make(map[string]string)
	for _, assetFilePath := range assetFilePaths {
		assetContent, err := readAssetFromFilePath(fileFS, assetFilePath)
		if err != nil {
			return nil, err
		}

		assetKey := strings.TrimPrefix(assetFilePath, dirPath)
		assetKey = strings.TrimPrefix(assetKey, "/")
		assetsMap[assetKey] = string(assetContent)
	}

	return assetsMap, nil
}

func readAssetFromFilePath(fileFS fs.FS, filePath string) ([]byte, error) {
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
