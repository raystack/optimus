package local

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/validator.v2"
	"gopkg.in/yaml.v2"
	"github.com/odpf/optimus/core/fs"
	"github.com/odpf/optimus/models"
)

const (
	SpecFileName    = "job.yaml"
	AssetFolderName = "assets"
)

var (
	specSuffixRegex = regexp.MustCompile(`\.(yaml|cfg|sql|txt)$`)
)

type jobRepository struct {
	fs    fs.FileSystem
	cache struct {
		dirty bool

		// cache is mapped with jobSpec name -> jobSpec
		data map[string]models.JobSpec
	}
	adapter *Adapter
}

func (repo *jobRepository) Save(job models.JobSpec) error {
	config, err := repo.adapter.FromSpec(job)
	if err != nil {
		return err
	}

	if err := validator.Validate(config); err != nil {
		return err
	}

	// save assets
	for assetName, assetValue := range config.Asset {
		assetFd, err := repo.fs.OpenForWrite(repo.assetFilePath(config.Name, assetName))
		if err != nil {
			return err
		}
		_, err = assetFd.Write([]byte(assetValue))
		if err != nil {
			return err
		}

		assetFd.Close()
	}
	config.Asset = nil

	// save job
	fileName := repo.jobFilePath(config.Name)
	fd, err := repo.fs.OpenForWrite(fileName)
	if err != nil {
		return err
	}
	defer fd.Close()

	if err := yaml.NewEncoder(fd).Encode(config); err != nil {
		return err
	}

	repo.cache.dirty = true
	return nil
}

// GetAll finds all the jobs recursively in current and sub directory
func (repo *jobRepository) GetAll() ([]models.JobSpec, error) {
	jobSpecs := []models.JobSpec{}
	if repo.cache.dirty {
		if err := repo.refreshCache(); err != nil {
			return jobSpecs, err
		}
	}

	for _, j := range repo.cache.data {
		jobSpecs = append(jobSpecs, j)
	}
	return jobSpecs, nil
}

// GetByName returns a job requested by the name
func (repo *jobRepository) GetByName(jobName string) (models.JobSpec, error) {
	jobSpec := models.JobSpec{}
	if strings.TrimSpace(jobName) == "" {
		return jobSpec, errors.Errorf("job name cannot be an empty string")
	}

	// refresh local cache if needed
	if repo.cache.dirty {
		if err := repo.refreshCache(); err != nil {
			return jobSpec, err
		}
	}

	// check if available in cache
	jobSpec, ok := repo.cache.data[jobName]
	if !ok {
		return jobSpec, models.ErrNoSuchSpec
	}
	return jobSpec, nil
}

// Delete deletes a requested job by name
func (repo *jobRepository) Delete(jobName string) error {
	panic("unimplemented")
	return nil
}

func (repo *jobRepository) refreshCache() error {
	repo.cache.dirty = true
	repo.cache.data = make(map[string]models.JobSpec)

	jobSpecs, err := repo.scanDirs(".")
	if err != nil && err != fs.ErrNoSuchFile {
		return err
	}
	if len(jobSpecs) < 1 {
		return models.ErrNoDAGSpecs
	}

	repo.cache.dirty = false
	return nil
}

func (repo *jobRepository) findInDir(dirName string) (models.JobSpec, error) {
	jobSpec := models.JobSpec{}
	if strings.TrimSpace(dirName) == "" {
		return jobSpec, fmt.Errorf("dir name cannot be an empty string")
	}

	fd, err := repo.fs.Open(repo.jobFilePath(dirName))
	if err != nil {
		if err == fs.ErrNoSuchFile {
			err = models.ErrNoSuchSpec
		}
		return jobSpec, err
	}
	defer fd.Close()

	var inputs Job
	dec := yaml.NewDecoder(fd)
	if err = dec.Decode(&inputs); err != nil {
		return jobSpec, errors.Wrapf(err, "error parsing job spec in %s", dirName)
	}
	if err := validator.Validate(inputs); err != nil {
		return jobSpec, errors.Wrapf(err, "failed to validate job specification: %s", dirName)
	}

	// convert to internal model
	jobSpec, err = repo.adapter.ToSpec(inputs)
	if err != nil {
		return jobSpec, errors.Wrapf(err, "failed to read spec in: %s", dirName)
	}

	assets := map[string]string{}
	assetFolderFd, err := repo.fs.Open(repo.assetFolderPath(dirName))
	if err == nil {
		fileNames, err := assetFolderFd.Readdirnames(-1)
		if err != nil {
			return jobSpec, err
		}
		for _, fileName := range fileNames {
			assetFd, err := repo.fs.Open(repo.assetFilePath(dirName, fileName))
			if err != nil {
				return jobSpec, err
			}

			raw, err := ioutil.ReadAll(assetFd)
			if err != nil {
				return jobSpec, err
			}
			assets[fileName] = string(raw)
			assetFd.Close()
		}
	}
	jobSpec.Assets = models.JobAssets{}.FromMap(assets)
	defer assetFolderFd.Close()

	if _, ok := repo.cache.data[jobSpec.Name]; ok {
		return jobSpec, errors.Errorf("job name should be unique across directories: %s", jobSpec.Name)
	}
	repo.cache.data[jobSpec.Name] = jobSpec
	return jobSpec, nil
}

func (repo *jobRepository) scanDirs(path string) ([]models.JobSpec, error) {
	specs := []models.JobSpec{}

	currentDir, err := repo.fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer currentDir.Close()

	fileNames, err := currentDir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	// filter folders & scan recursively
	folders := repo.getDirs(fileNames)
	for _, folder := range folders {
		s, err := repo.scanDirs(filepath.Join(path, folder))
		if err != nil && err != fs.ErrNoSuchFile {
			return s, err
		}
		specs = append(specs, s...)
	}

	// find job in this folder
	spec, err := repo.findInDir(path)
	if err != nil {
		if err != fs.ErrNoSuchFile && err != models.ErrNoSuchSpec {
			return nil, err
		}
	} else {
		specs = append(specs, spec)
	}

	return specs, nil
}

func (repo *jobRepository) getDirs(paths []string) []string {
	folderPath := []string{}
	for _, path := range paths {
		if strings.HasPrefix(path, ".") {
			continue
		}
		if specSuffixRegex.FindString(path) == "" && path != AssetFolderName {
			folderPath = append(folderPath, path)
		}
	}
	return folderPath
}

// jobFilePath generates the filename for a given job
func (repo *jobRepository) jobFilePath(name string) string {
	return filepath.Join(name, SpecFileName)
}

// assetFolderPath generates the directory for a given job that
// contains attached asset files
func (repo *jobRepository) assetFolderPath(name string) string {
	return filepath.Join(name, AssetFolderName)
}

// assetFilePath generates the path to asset directory files
// for a given job
func (repo *jobRepository) assetFilePath(job string, file string) string {
	return filepath.Join(repo.assetFolderPath(job), file)
}

func NewJobSpecRepository(fs fs.FileSystem, adapter *Adapter) *jobRepository {
	repo := new(jobRepository)
	repo.fs = fs
	repo.cache.dirty = true
	repo.adapter = adapter
	return repo
}
