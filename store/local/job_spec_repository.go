package local

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/afero"
	"gopkg.in/validator.v2"
	"gopkg.in/yaml.v2"

	"github.com/odpf/optimus/models"
)

const (
	JobSpecParentName = "this.yaml"
	JobSpecFileName   = "job.yaml"
	AssetFolderName   = "assets"
)

var specSuffixRegex = regexp.MustCompile(`\.(yaml|cfg|sql|txt)$`)

type cacheItem struct {
	path string
	item interface{}
}

type jobRepository struct {
	fs    afero.Fs
	cache struct {
		dirty bool

		// cache is mapped with jobSpec name -> cacheItem
		data map[string]cacheItem
	}
	adapter *JobSpecAdapter
}

func (repo *jobRepository) SaveAt(job models.JobSpec, rootDir string) error {
	config, err := repo.adapter.FromSpec(job)
	if err != nil {
		return fmt.Errorf("repo.adapter.FromJobSpec: %s: %w", config.Name, err)
	}

	if err := validator.Validate(config); err != nil {
		return fmt.Errorf("validator.Validate: %s: %w", config.Name, err)
	}

	// set default dir name as config name
	if rootDir == "" {
		rootDir = config.Name
	}

	// create necessary folders
	if err = repo.fs.MkdirAll(repo.assetFolderPath(rootDir), os.FileMode(0o765)|os.ModeDir); err != nil {
		return fmt.Errorf("repo.fs.MkdirAll: %s: %w", rootDir, err)
	}

	// save assets
	for assetName, assetValue := range config.Asset {
		if err := afero.WriteFile(repo.fs, repo.assetFilePath(rootDir, assetName), []byte(assetValue), os.FileMode(0o755)); err != nil {
			return fmt.Errorf("error in writing asset: %s: %w", repo.assetFilePath(rootDir, assetName), err)
		}
	}
	config.Asset = nil

	// save job
	fd, err := repo.fs.OpenFile(repo.jobFilePath(rootDir), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(0o755))
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

func (repo *jobRepository) Save(job models.JobSpec) error {
	if job.Name == "" {
		return errors.New("invalid job name")
	}
	// refresh local cache if needed, going to need this to find existing spec paths
	if repo.cache.dirty {
		if err := repo.refreshCache(); err != nil {
			return err
		}
	}

	specDir := ""
	// check if we are updating an existing spec
	existingJob, ok := repo.cache.data[job.Name]
	if ok {
		specDir = existingJob.path
	}
	return repo.SaveAt(job, specDir)
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
		jobSpecs = append(jobSpecs, j.item.(models.JobSpec))
	}
	if len(jobSpecs) < 1 {
		return nil, models.ErrNoJobs
	}
	return jobSpecs, nil
}

// GetByName returns a job requested by the name
func (repo *jobRepository) GetByName(jobName string) (models.JobSpec, error) {
	if strings.TrimSpace(jobName) == "" {
		return models.JobSpec{}, fmt.Errorf("job name cannot be an empty string")
	}

	// refresh local cache if needed
	if repo.cache.dirty {
		if err := repo.refreshCache(); err != nil {
			return models.JobSpec{}, err
		}
	}

	// check if available in cache
	blob, ok := repo.cache.data[jobName]
	if !ok {
		return models.JobSpec{}, models.ErrNoSuchSpec
	}
	return blob.item.(models.JobSpec), nil
}

func (*jobRepository) GetByDestination(string) (models.JobSpec, models.ProjectSpec, error) {
	panic("GetByDestination() should not be invoked with local.JobSpecRepo")
}

// Delete deletes a requested job by name
func (*jobRepository) Delete(string) error {
	panic("unimplemented")
}

func (repo *jobRepository) refreshCache() error {
	repo.cache.dirty = true
	repo.cache.data = make(map[string]cacheItem)

	_, err := repo.scanDirs(".", Job{})
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	repo.cache.dirty = false
	return nil
}

func (repo *jobRepository) findInDir(dirName string, inheritedSpec Job) (models.JobSpec, error) {
	jobSpec := models.JobSpec{}
	if strings.TrimSpace(dirName) == "" {
		return jobSpec, fmt.Errorf("dir name cannot be an empty string")
	}

	fd, err := repo.fs.Open(repo.jobFilePath(dirName))
	if err != nil {
		if os.IsNotExist(err) {
			err = models.ErrNoSuchSpec
		}
		return jobSpec, err
	}
	defer fd.Close()

	dec := yaml.NewDecoder(fd)
	var inputs Job
	if err = dec.Decode(&inputs); err != nil {
		return jobSpec, fmt.Errorf("error parsing job spec in %s: %w", dirName, err)
	}
	inputs.MergeFrom(inheritedSpec)
	if err := validator.Validate(inputs); err != nil {
		return jobSpec, fmt.Errorf("failed to validate job specification: %s: %w", dirName, err)
	}

	// convert to internal model
	jobSpec, err = repo.adapter.ToSpec(inputs)
	if err != nil {
		return jobSpec, fmt.Errorf("failed to read spec in: %s: %w", dirName, err)
	}

	assets := map[string]string{}
	assetFolderFd, err := repo.fs.Open(repo.assetFolderPath(dirName))
	if err == nil {
		fileNames, err := assetFolderFd.Readdirnames(-1)
		if err != nil {
			return jobSpec, err
		}
		for _, fileName := range fileNames {
			// skip directories in assets folder
			if isDir, err := afero.IsDir(repo.fs, repo.assetFilePath(dirName, fileName)); err == nil && isDir {
				continue
			} else if err != nil {
				return jobSpec, err
			}

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
		defer assetFolderFd.Close()
	}
	jobSpec.Assets = models.JobAssets{}.FromMap(assets)

	if _, ok := repo.cache.data[jobSpec.Name]; ok {
		return jobSpec, fmt.Errorf("job name should be unique across directories: %s: %w", jobSpec.Name, err)
	}
	repo.cache.data[jobSpec.Name] = cacheItem{
		path: dirName,
		item: jobSpec,
	}
	return jobSpec, nil
}

func (repo *jobRepository) scanDirs(path string, inheritedSpec Job) ([]models.JobSpec, error) {
	specs := []models.JobSpec{}

	// find this config
	thisSpec, err := repo.getThisSpec(path)
	if err != nil {
		return nil, err
	}
	thisSpec.MergeFrom(inheritedSpec)

	// filter folders & scan recursively
	folders, err := repo.getDirs(path)
	if err != nil {
		return nil, err
	}
	for _, folder := range folders {
		s, err := repo.scanDirs(filepath.Join(path, folder), thisSpec)
		if err != nil && !os.IsNotExist(err) {
			return s, err
		}
		specs = append(specs, s...)
	}

	// find job in this folder
	spec, err := repo.findInDir(path, thisSpec)
	if err != nil {
		if !os.IsNotExist(err) && !errors.Is(err, models.ErrNoSuchSpec) {
			return nil, err
		}
	} else {
		specs = append(specs, spec)
	}

	return specs, nil
}

func (repo *jobRepository) getThisSpec(dirName string) (Job, error) {
	fd, err := repo.fs.Open(repo.thisFilePath(dirName))
	if err != nil {
		if os.IsNotExist(err) {
			return Job{}, nil
		}
		return Job{}, err
	}
	defer fd.Close()

	// prepare a clone
	var inputs Job
	dec := yaml.NewDecoder(fd)
	if err = dec.Decode(&inputs); err != nil {
		return Job{}, fmt.Errorf("error parsing job spec in %s: %w", dirName, err)
	}
	return inputs, nil
}

// getDirs return names of all the folders in provided path
func (repo *jobRepository) getDirs(dirPath string) ([]string, error) {
	currentDir, err := repo.fs.Open(dirPath)
	if err != nil {
		return nil, err
	}

	fileNames, err := currentDir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	currentDir.Close()

	folderPath := []string{}
	for _, fileName := range fileNames {
		if strings.HasPrefix(fileName, ".") {
			continue
		}
		if specSuffixRegex.FindString(fileName) != "" || fileName == AssetFolderName {
			continue
		}

		if isDir, err := afero.IsDir(repo.fs, filepath.Join(dirPath, fileName)); err == nil && !isDir {
			continue
		} else if err != nil {
			return nil, err
		}

		folderPath = append(folderPath, fileName)
	}
	return folderPath, nil
}

// thisFilePath generates the filename for this specification which will be inherited by
// all children
func (*jobRepository) thisFilePath(name string) string {
	return filepath.Join(name, JobSpecParentName)
}

// jobFilePath generates the filename for a given job
func (*jobRepository) jobFilePath(name string) string {
	return filepath.Join(name, JobSpecFileName)
}

// assetFolderPath generates the directory for a given job that
// contains attached asset files
func (*jobRepository) assetFolderPath(name string) string {
	return filepath.Join(name, AssetFolderName)
}

// assetFilePath generates the path to asset directory files
// for a given job
func (repo *jobRepository) assetFilePath(job, file string) string {
	return filepath.Join(repo.assetFolderPath(job), file)
}

func NewJobSpecRepository(fs afero.Fs, adapter *JobSpecAdapter) *jobRepository {
	repo := new(jobRepository)
	repo.fs = fs
	repo.cache.dirty = true
	repo.adapter = adapter
	return repo
}
