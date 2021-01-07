package gcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"google.golang.org/api/iterator"
	"github.com/odpf/optimus/models"
)

var (
	errEmptyJobName = errors.New("job name cannot be an empty string")
)

type JobRepository struct {
	ObjectReader objectReader
	ObjectWriter objectWriter
	Client       stiface.Client
	Bucket       string
	Prefix       string
	Suffix       string

	fileExtension string
}

func (repo *JobRepository) Save(j models.Job) error {
	dst, err := repo.ObjectWriter.NewWriter(repo.Bucket, repo.pathFor(j))
	if err != nil {
		return err
	}
	defer dst.Close()
	src := bytes.NewBuffer(j.Contents)
	_, err = io.Copy(dst, src)
	return err
}

func (repo *JobRepository) Delete(jobName string) error {
	ctx := context.Background()
	if strings.TrimSpace(jobName) == "" {
		return errEmptyJobName
	}

	bucket := repo.Client.Bucket(repo.Bucket)
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return err
	}

	filePath := fmt.Sprintf("%s%s", path.Join(repo.Prefix, jobName), repo.Suffix)
	objectHandle := bucket.Object(filePath)
	_, err = objectHandle.Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return models.ErrNoSuchJob
		}
		return err
	}

	err = objectHandle.Delete(context.Background())
	if err != nil {
		return err
	}

	return nil
}

func (repo *JobRepository) GetAll() ([]models.Job, error) {
	ctx := context.Background()

	bucket := repo.Client.Bucket(repo.Bucket)
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return nil, err
	}

	query := storage.Query{
		Prefix: repo.Prefix,
	}
	it := bucket.Objects(ctx, &query)

	var objAttrs []*storage.ObjectAttrs
	for {
		objAttr, err := it.Next()

		if err != nil && err != iterator.Done {
			return nil, err
		}

		if err == iterator.Done {
			break
		}

		if strings.HasSuffix(objAttr.Name, repo.Suffix) {
			objAttrs = append(objAttrs, objAttr)
		}
	}

	var jobs []models.Job
	for _, objAttr := range objAttrs {
		reader, err := repo.ObjectReader.NewReader(repo.Bucket, objAttr.Name)
		if err != nil {
			return nil, err
		}
		defer reader.Close()

		var b bytes.Buffer
		_, err = b.ReadFrom(reader)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, models.Job{
			Name:     repo.jobNameFromPath(objAttr.Name),
			Contents: b.Bytes(),
		})
	}

	return jobs, nil
}

func (repo *JobRepository) GetByName(jobName string) (models.Job, error) {
	if strings.TrimSpace(jobName) == "" {
		return models.Job{}, errEmptyJobName
	}

	ctx := context.Background()
	bucket := repo.Client.Bucket(repo.Bucket)
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return models.Job{}, err
	}

	filePath := fmt.Sprintf("%s%s", path.Join(repo.Prefix, jobName), repo.Suffix)

	objHandle := bucket.Object(filePath)
	_, err = objHandle.Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return models.Job{}, models.ErrNoSuchJob
		}
		return models.Job{}, err
	}

	reader, err := repo.ObjectReader.NewReader(repo.Bucket, filePath)
	if err != nil {
		return models.Job{}, err
	}
	defer reader.Close()

	var b bytes.Buffer
	_, err = b.ReadFrom(reader)
	if err != nil {
		return models.Job{}, err
	}

	return models.Job{
		Name:     jobName,
		Contents: b.Bytes(),
	}, nil
}

func (repo *JobRepository) pathFor(j models.Job) string {
	if len(repo.Prefix) > 0 && repo.Prefix[0] == '/' {
		repo.Prefix = repo.Prefix[1:]
	}
	return fmt.Sprintf("%s%s", path.Join(repo.Prefix, j.Name), repo.Suffix)
}

func (repo *JobRepository) jobNameFromPath(filePath string) string {
	jobFileName := path.Base(filePath)
	return strings.ReplaceAll(jobFileName, repo.Suffix, "")
}

func cleanPrefix(prefix string) string {
	prefix = strings.TrimPrefix(prefix, "/")
	if strings.HasSuffix(prefix, "/") == false {
		prefix += "/"
	}
	return prefix
}

// NewJobRepository constructs a new GCSRepository client
func NewJobRepository(bucket, prefix, sufix string, c *storage.Client) *JobRepository {
	return &JobRepository{
		ObjectReader: &gcsObjectReader{c},
		ObjectWriter: &gcsObjectWriter{c},
		Client:       stiface.AdaptClient(c),
		Bucket:       bucket,
		Prefix:       cleanPrefix(prefix),
		Suffix:       sufix,
	}
}
