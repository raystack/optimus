package specio

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"

	"github.com/odpf/optimus/client/local"
	"github.com/odpf/optimus/client/local/internal"
	"github.com/odpf/optimus/client/local/model"
)

type jobSpecReadWriter struct {
	withParentReading bool

	referenceParentFileName string
	referenceSpecFileName   string
	referenceAssetDirName   string

	specFS afero.Fs
}

func NewJobSpecReadWriter(specFS afero.Fs, opts ...jobSpecReadWriterOpt) (local.SpecReadWriter[*model.JobSpec], error) {
	if specFS == nil {
		return nil, errors.New("specFS is nil")
	}
	j := &jobSpecReadWriter{
		withParentReading:       false,
		referenceParentFileName: "this.yaml",
		referenceSpecFileName:   "job.yaml",
		referenceAssetDirName:   "assets",
		specFS:                  specFS,
	}

	for _, opt := range opts {
		if err := opt(j); err != nil {
			return nil, err
		}
	}

	return j, nil
}

func (j jobSpecReadWriter) ReadAll(rootDirPath string) ([]*model.JobSpec, error) {
	if rootDirPath == "" {
		return nil, errors.New("root dir path is empty")
	}

	jobSpecParentsMappedByDirPath, err := j.readJobSpecParentsMappedByDirPath(rootDirPath)
	if err != nil {
		return nil, fmt.Errorf("error reading parent specs under [%s]: %w", rootDirPath, err)
	}

	dirPaths, err := internal.DiscoverSpecDirPaths(j.specFS, rootDirPath, j.referenceSpecFileName)
	if err != nil {
		return nil, fmt.Errorf("error discovering spec dir paths under [%s]: %w", rootDirPath, err)
	}
	jobSpecs := make([]*model.JobSpec, len(dirPaths))
	for i, dirPath := range dirPaths {
		jobSpec, err := j.readJobSpec(dirPath)
		if err != nil {
			return nil, fmt.Errorf("error reading job spec under [%s]: %w", dirPath, err)
		}
		if j.withParentReading {
			j.mergeJobSpecWithParents(jobSpec, dirPath, jobSpecParentsMappedByDirPath)
		}
		jobSpecs[i] = jobSpec
	}
	return jobSpecs, nil
}

func (j jobSpecReadWriter) ReadByName(rootDirPath, name string) (*model.JobSpec, error) {
	if name == "" {
		return nil, errors.New("name is empty")
	}
	allSpecs, err := j.ReadAll(rootDirPath)
	if err != nil {
		return nil, fmt.Errorf("error reading all specs under [%s]: %w", rootDirPath, err)
	}
	spec := internal.GetFirstSpecByFilter(allSpecs, func(js *model.JobSpec) bool { return js.Name == name })
	if spec == nil {
		return nil, fmt.Errorf("spec with name [%s] is not found", name)
	}
	return spec, nil
}

func (j jobSpecReadWriter) Write(dirPath string, spec *model.JobSpec) error {
	if dirPath == "" {
		return errors.New("dir path is empty")
	}
	if spec == nil {
		return errors.New("job spec is nil")
	}

	specFilePath := filepath.Join(dirPath, j.referenceSpecFileName)
	if err := internal.WriteSpec(j.specFS, specFilePath, spec); err != nil {
		return fmt.Errorf("error writing spec into [%s]: %w", specFilePath, err)
	}
	for assetFileName, assetContent := range spec.Asset {
		assetFilePath := filepath.Join(dirPath, j.referenceAssetDirName, assetFileName)
		if err := j.writeJobSpecAsset(assetFilePath, assetContent); err != nil {
			return fmt.Errorf("error writing asset into [%s]: %w", assetFilePath, err)
		}
	}
	return nil
}

func (j jobSpecReadWriter) writeJobSpecAsset(filePath, content string) error {
	if err := j.specFS.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return err
	}
	f, err := j.specFS.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating asset file into [%s]: %w", filePath, err)
	}
	defer f.Close()
	_, err = f.WriteString(content)
	return err
}

func (jobSpecReadWriter) mergeJobSpecWithParents(spec *model.JobSpec, specDirPath string, jobSpecParentsMappedByDirPath map[string]*model.JobSpec) {
	splitDirPaths := strings.Split(specDirPath, "/")
	for i := range splitDirPaths {
		pathNearSpecIdx := len(splitDirPaths) - i
		rootToNearSpecPaths := splitDirPaths[:pathNearSpecIdx]
		parentDirPath := filepath.Join(rootToNearSpecPaths...)
		if parentJobSpec, ok := jobSpecParentsMappedByDirPath[parentDirPath]; ok {
			spec.MergeFrom(parentJobSpec)
		}
	}
}

func (j jobSpecReadWriter) readJobSpecParentsMappedByDirPath(rootDirPath string) (map[string]*model.JobSpec, error) {
	parentDirPaths, err := internal.DiscoverSpecDirPaths(j.specFS, rootDirPath, j.referenceParentFileName)
	if err != nil {
		return nil, fmt.Errorf("error discovering parent spec paths under [%s]: %w", rootDirPath, err)
	}
	parentsMappedByDirPath := make(map[string]*model.JobSpec)
	for _, dirPath := range parentDirPaths {
		filePath := filepath.Join(dirPath, j.referenceParentFileName)
		spec, err := internal.ReadSpec[*model.JobSpec](j.specFS, filePath)
		if err != nil {
			return nil, fmt.Errorf("error reading spec under [%s]: %w", filePath, err)
		}
		spec.Path = dirPath
		parentsMappedByDirPath[dirPath] = spec
	}
	return parentsMappedByDirPath, nil
}

func (j jobSpecReadWriter) readJobSpec(dirPath string) (*model.JobSpec, error) {
	specFilePath := filepath.Join(dirPath, j.referenceSpecFileName)
	jobSpec, err := internal.ReadSpec[*model.JobSpec](j.specFS, specFilePath)
	if err != nil {
		return nil, fmt.Errorf("error reading spec under [%s]: %w", dirPath, err)
	}
	assetsMappedByFileName, err := j.readJobSpecAssetsMappedByFileName(dirPath)
	if err != nil {
		return nil, fmt.Errorf("error reading asset under [%s]: %w", dirPath, err)
	}
	jobSpec.Asset = assetsMappedByFileName
	jobSpec.Path = dirPath
	return jobSpec, nil
}

func (j jobSpecReadWriter) readJobSpecAssetsMappedByFileName(dirPath string) (map[string]string, error) {
	assetDirPath := filepath.Join(dirPath, j.referenceAssetDirName)
	assetsMap := make(map[string]string)
	if _, err := j.specFS.Stat(assetDirPath); errors.Is(err, fs.ErrNotExist) {
		return assetsMap, nil
	} else if err != nil {
		return nil, err
	}

	assetFilePaths, err := internal.DiscoverFilePaths(j.specFS, assetDirPath)
	if err != nil {
		return nil, fmt.Errorf("error discovering asset file paths under [%s]: %w", assetDirPath, err)
	}

	for _, assetFilePath := range assetFilePaths {
		assetContent, err := j.readJobSpecAssetFile(assetFilePath)
		if err != nil {
			return nil, fmt.Errorf("error reading asset file under [%s]: %w", assetFilePath, err)
		}
		assetFileName := strings.TrimPrefix(assetFilePath, assetDirPath)
		assetFileName = strings.TrimPrefix(assetFileName, "/")
		assetsMap[assetFileName] = string(assetContent)
	}
	return assetsMap, nil
}

func (j jobSpecReadWriter) readJobSpecAssetFile(assetFilePath string) ([]byte, error) {
	f, err := j.specFS.Open(assetFilePath)
	if err != nil {
		return nil, fmt.Errorf("error opening asset file under [%s]: %w", assetFilePath, err)
	}
	defer f.Close()

	return io.ReadAll(f)
}
