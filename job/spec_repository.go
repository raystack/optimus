package job

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

type SpecRepository struct {
	fs    fs.FileSystem
	fac   models.JobSpecFactory
	cache struct {
		dirty bool
		data  map[string]models.JobSpec
	}
}

func (repo *SpecRepository) Save(job models.JobInput) error {
	if err := validator.Validate(job); err != nil {
		return err
	}

	// save assets
	for assetName, assetValue := range job.Asset {
		assetFd, err := repo.fs.Create(repo.assetFilePath(job.Name, assetName))
		if err != nil {
			return err
		}
		_, err = assetFd.Write([]byte(assetValue))
		if err != nil {
			return err
		}

		assetFd.Close()
	}
	job.Asset = nil

	// save job
	fileName := repo.jobFilePath(job.Name)
	fd, err := repo.fs.Create(fileName)
	if err != nil {
		return err
	}
	defer fd.Close()
	if err := yaml.NewEncoder(fd).Encode(job); err != nil {
		return err
	}

	repo.cache.dirty = true
	return nil
}

func (repo *SpecRepository) GetByName(jobName string) (models.JobSpec, error) {
	jobSpec := models.JobSpec{}
	if strings.TrimSpace(jobName) == "" {
		return jobSpec, fmt.Errorf("Job name cannot be an empty string")
	}

	// check if cache is ready for job
	if val, ok := repo.cache.data[jobName]; ok {
		return val, nil
	}

	fd, err := repo.fs.Open(repo.jobFilePath(jobName))
	if err != nil {
		if err == fs.ErrNoSuchFile {
			err = models.ErrNoSuchSpec
		}
		return jobSpec, err
	}
	defer fd.Close()

	var inputs models.JobInput
	dec := yaml.NewDecoder(fd)
	if err = dec.Decode(&inputs); err != nil {
		return jobSpec, fmt.Errorf("error parsing dag spec %s: %v", jobName, err)
	}
	if err := validator.Validate(inputs); err != nil {
		return jobSpec, errors.Wrapf(err, "failed to validate job specification: %s", jobName)
	}

	// convert to internal model
	jobSpec, err = repo.fac.CreateJobSpec(inputs)
	if err != nil {
		return jobSpec, errors.Wrapf(err, "failed to read spec in: %s", jobName)
	}

	jobSpec.Asset = map[string]string{}
	assetFolderFd, err := repo.fs.Open(repo.assetFolderPath(jobName))
	if err == nil {
		fileNames, err := assetFolderFd.Readdirnames(-1)
		if err != nil {
			return jobSpec, err
		}
		for _, fileName := range fileNames {
			assetFd, err := repo.fs.Open(repo.assetFilePath(jobName, fileName))
			if err != nil {
				return jobSpec, err
			}

			raw, err := ioutil.ReadAll(assetFd)
			if err != nil {
				return jobSpec, err
			}
			jobSpec.Asset[fileName] = string(raw)
			assetFd.Close()
		}
	}
	defer assetFolderFd.Close()

	repo.cache.data[jobName] = jobSpec
	return jobSpec, nil
}

// GetAll finds all the jobs recursively in current and sub directory
func (repo *SpecRepository) GetAll() ([]models.JobSpec, error) {
	jobSpecs := []models.JobSpec{}
	var err error
	if !repo.cache.dirty {
		for _, j := range repo.cache.data {
			jobSpecs = append(jobSpecs, j)
		}
		return jobSpecs, nil
	}
	repo.resetCache()

	jobSpecs, err = repo.scanDirs(".")
	if err != nil && err != fs.ErrNoSuchFile {
		return jobSpecs, err
	}
	if len(jobSpecs) < 1 {
		return nil, models.ErrNoDAGSpecs
	}

	repo.cache.dirty = false
	return jobSpecs, err
}

func (repo *SpecRepository) scanDirs(path string) ([]models.JobSpec, error) {
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
	spec, err := repo.GetByName(path)
	if err != nil {
		if err != fs.ErrNoSuchFile && err != models.ErrNoSuchSpec {
			return nil, err
		}
	} else {
		specs = append(specs, spec)
	}

	return specs, nil
}

func (repo *SpecRepository) getDirs(paths []string) []string {
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
func (repo *SpecRepository) jobFilePath(name string) string {
	return filepath.Join(name, SpecFileName)
}

// assetFolderPath generates the directory for a given job that
// contains attached asset files
func (repo *SpecRepository) assetFolderPath(name string) string {
	return filepath.Join(name, AssetFolderName)
}

// assetFilePath generates the path to asset directory files
// for a given job
func (repo *SpecRepository) assetFilePath(job string, file string) string {
	return filepath.Join(repo.assetFolderPath(job), file)
}

func (repo *SpecRepository) resetCache() {
	repo.cache.data = make(map[string]models.JobSpec)
	repo.cache.dirty = true
}

func NewSpecRepository(fs fs.FileSystem, fac models.JobSpecFactory) *SpecRepository {
	repo := new(SpecRepository)
	repo.fs = fs
	repo.fac = fac
	repo.resetCache()
	return repo
}

// func NewSpecRepositoryAt(BasePath string) *SpecRepository {
// 	return NewSpecRepository(&fs.LocalFileSystem{BasePath: BasePath})
// }
