package gcs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

var (
	errEmptyJobName = errors.New("job name cannot be an empty string")
)

type JobRepository struct {
	ObjectReader store.ObjectReader
	ObjectWriter store.ObjectWriter
	Client       stiface.Client
	Bucket       string
	Prefix       string
	Suffix       string
}

func (repo *JobRepository) Save(ctx context.Context, j models.Job) (err error) {
	dst, err := repo.ObjectWriter.NewWriter(ctx, repo.Bucket, repo.pathFor(j))
	if err != nil {
		return err
	}
	defer func() {
		if derr := dst.Close(); derr != nil {
			if err == nil {
				err = derr
			} else {
				err = errors.Wrap(err, derr.Error())
			}
		}
	}()
	src := bytes.NewBuffer(j.Contents)
	_, err = io.Copy(dst, src)
	return err
}

func (repo *JobRepository) Delete(ctx context.Context, namespace models.NamespaceSpec, jobName string) error {
	if strings.TrimSpace(jobName) == "" {
		return errEmptyJobName
	}

	bucket := repo.Client.Bucket(repo.Bucket)
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return err
	}

	filePath := fmt.Sprintf("%s%s", path.Join(repo.Prefix, namespace.ID.String(), jobName), repo.Suffix)
	objectHandle := bucket.Object(filePath)
	_, err = objectHandle.Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return errors.Wrap(models.ErrNoSuchJob, jobName)
		}
		return err
	}

	err = objectHandle.Delete(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (repo *JobRepository) GetAll(ctx context.Context) ([]models.Job, error) {
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

func (repo *JobRepository) ListNames(ctx context.Context, namespace models.NamespaceSpec) ([]string, error) {
	bucket := repo.Client.Bucket(repo.Bucket)
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return nil, err
	}

	query := storage.Query{
		Prefix: path.Join(repo.Prefix, namespace.ID.String()),
	}
	it := bucket.Objects(ctx, &query)

	var jobNames []string
	for {
		objAttr, err := it.Next()
		if err != nil && err != iterator.Done {
			return nil, err
		}
		if err == iterator.Done {
			break
		}

		if strings.HasSuffix(objAttr.Name, repo.Suffix) {
			jobNames = append(jobNames, repo.jobNameFromPath(objAttr.Name))
		}
	}
	return jobNames, nil
}

func (repo *JobRepository) GetByName(ctx context.Context, jobName string) (models.Job, error) {
	if strings.TrimSpace(jobName) == "" {
		return models.Job{}, errEmptyJobName
	}

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
			return models.Job{}, errors.Wrap(models.ErrNoSuchJob, jobName)
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
	return fmt.Sprintf("%s%s", path.Join(repo.Prefix, j.NamespaceID, j.Name), repo.Suffix)
}

func (repo *JobRepository) jobNameFromPath(filePath string) string {
	jobFileName := path.Base(filePath)
	return strings.ReplaceAll(jobFileName, repo.Suffix, "")
}

func cleanPrefix(prefix string) string {
	prefix = strings.TrimPrefix(prefix, "/")
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return prefix
}

// NewJobRepository constructs a new GCSRepository client
func NewJobRepository(bucket, prefix, suffix string, c *storage.Client) *JobRepository {
	return &JobRepository{
		ObjectReader: &gcsObjectReader{c},
		ObjectWriter: &GcsObjectWriter{c},
		Client:       stiface.AdaptClient(c),
		Bucket:       bucket,
		Prefix:       cleanPrefix(prefix),
		Suffix:       suffix,
	}
}
