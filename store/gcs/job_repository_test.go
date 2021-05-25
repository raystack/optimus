package gcs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	mocked "github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	gcsStore "github.com/odpf/optimus/store/gcs"
)

func TestJobRepository(t *testing.T) {
	ctx := context.Background()
	t.Run("Save", func(t *testing.T) {
		testJob := models.Job{
			Name:     "test",
			Contents: []byte("print('this is a job')"),
		}
		ctx := context.Background()
		t.Run("should write job contents to destination bucket", func(t *testing.T) {

			bucket := "scheduled-tasks"
			prefix := "resources/jobs"

			var out bytes.Buffer
			wc := new(mocked.WriteCloser)
			defer wc.AssertExpectations(t)
			wc.On("Write").Return(&out, nil)
			wc.On("Close").Return(nil)

			ow := new(mocked.ObjectWriter)
			defer ow.AssertExpectations(t)

			objectPath := fmt.Sprintf("%s/%s", prefix, testJob.Name)
			ow.On("NewWriter", ctx, bucket, objectPath).Return(wc, nil)

			repo := &gcsStore.JobRepository{
				ObjectWriter: ow,
				Bucket:       bucket,
				Prefix:       prefix,
			}

			err := repo.Save(ctx, testJob)
			assert.Nil(t, err)
			assert.Equal(t, string(testJob.Contents), out.String())
		})
		t.Run("should write job contents to destination bucket with suffix", func(t *testing.T) {

			bucket := "scheduled-tasks"
			prefix := "resources/jobs"

			var out bytes.Buffer
			wc := new(mocked.WriteCloser)
			defer wc.AssertExpectations(t)
			wc.On("Write").Return(&out, nil)
			wc.On("Close").Return(nil)

			ow := new(mocked.ObjectWriter)
			defer ow.AssertExpectations(t)

			objectPath := fmt.Sprintf("%s/%s%s", prefix, testJob.Name, ".py")
			ow.On("NewWriter", ctx, bucket, objectPath).Return(wc, nil)

			repo := &gcsStore.JobRepository{
				ObjectWriter: ow,
				Bucket:       bucket,
				Prefix:       prefix,
				Suffix:       ".py",
			}

			err := repo.Save(ctx, testJob)
			assert.Nil(t, err)
			assert.Equal(t, string(testJob.Contents), out.String())
		})
		t.Run("should return error if writing to object fails", func(t *testing.T) {
			writeError := errors.New("write error")
			bucket := "foo"
			prefix := "bar"

			wc := new(mocked.WriteCloser)
			defer wc.AssertExpectations(t)
			wc.On("Write").Return(new(bytes.Buffer), writeError)
			wc.On("Close").Return(nil)

			ow := new(mocked.ObjectWriter)
			objPath := fmt.Sprintf("%s/%s", prefix, testJob.Name)
			ow.On("NewWriter", ctx, bucket, objPath).Return(wc, nil)

			repo := gcsStore.JobRepository{
				ObjectWriter: ow,
				Bucket:       bucket,
				Prefix:       prefix,
			}

			err := repo.Save(ctx, testJob)
			assert.Equal(t, writeError, err)
		})
		t.Run("should return error if opening the object fails", func(t *testing.T) {
			bucketError := errors.New("bucket does not exist")
			bucket := "foo"
			prefix := "bar"
			ow := new(mocked.ObjectWriter)
			objPath := fmt.Sprintf("%s/%s", prefix, testJob.Name)
			ow.On("NewWriter", ctx, bucket, objPath).Return(new(mocked.WriteCloser), bucketError)
			defer ow.AssertExpectations(t)
			repo := gcsStore.JobRepository{
				ObjectWriter: ow,
				Bucket:       bucket,
				Prefix:       prefix,
			}
			err := repo.Save(ctx, testJob)
			assert.Equal(t, bucketError, err)
		})
	})
	t.Run("Delete", func(t *testing.T) {
		jobName := "job-1"
		prefix := "resources/jobs"
		bucket := "scheduled-bucket"

		t.Run("remove DAG object from repository", func(t *testing.T) {
			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "dev-team-1",
			}

			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, nil)

			objectHandle := new(objectHandleMock)
			defer objectHandle.AssertExpectations(t)
			objectHandle.On("Attrs", context.Background()).Return(&storage.ObjectAttrs{}, nil)
			objectHandle.On("Delete", context.Background()).Return(nil)

			filePath := fmt.Sprintf("%s/%s/%s", prefix, namespaceSpec.ID, jobName)
			bucketHandle.On("Object", filePath).Return(objectHandle)
			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				Client: client,
				Bucket: bucket,
				Prefix: prefix,
			}
			err := repo.Delete(ctx, namespaceSpec, jobName)

			assert.Nil(t, err)
		})
		t.Run("should return error when jobName is empty", func(t *testing.T) {
			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "dev-team-1",
			}

			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			repo := &gcsStore.JobRepository{
				Client: client,
				Bucket: bucket,
				Prefix: prefix,
			}
			err := repo.Delete(ctx, namespaceSpec, "")
			assert.NotNil(t, err)
		})
		t.Run("should return ErrNoSuchDAG when job is not exist", func(t *testing.T) {
			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "dev-team-1",
			}

			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, nil)

			objectHandle := new(objectHandleMock)
			defer objectHandle.AssertExpectations(t)
			objectHandle.On("Attrs", context.Background()).Return(&storage.ObjectAttrs{}, storage.ErrObjectNotExist)

			filePath := fmt.Sprintf("%s/%s/%s", prefix, namespaceSpec.ID, jobName)
			bucketHandle.On("Object", filePath).Return(objectHandle)
			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				Client: client,
				Bucket: bucket,
				Prefix: prefix,
			}
			err := repo.Delete(ctx, namespaceSpec, jobName)

			assert.Error(t, models.ErrNoSuchJob, err)
		})
		t.Run("should return err when unable to get the object info", func(t *testing.T) {
			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "dev-team-1",
			}

			anotherError := errors.New("another error")
			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, nil)

			objectHandle := new(objectHandleMock)
			defer objectHandle.AssertExpectations(t)
			objectHandle.On("Attrs", context.Background()).Return(&storage.ObjectAttrs{}, anotherError)

			filePath := fmt.Sprintf("%s/%s/%s", prefix, namespaceSpec.ID, jobName)
			bucketHandle.On("Object", filePath).Return(objectHandle)
			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				Client: client,
				Bucket: bucket,
				Prefix: prefix,
			}
			err := repo.Delete(ctx, namespaceSpec, jobName)

			assert.Equal(t, anotherError, err)
		})
		t.Run("should return error when deletion the object failed", func(t *testing.T) {
			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "dev-team-1",
			}

			anError := errors.New("failed to delete object")
			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, nil)

			objectHandle := new(objectHandleMock)
			defer objectHandle.AssertExpectations(t)
			objectHandle.On("Attrs", context.Background()).Return(&storage.ObjectAttrs{}, nil)
			objectHandle.On("Delete", context.Background()).Return(anError)

			filePath := fmt.Sprintf("%s/%s/%s", prefix, namespaceSpec.ID, jobName)
			bucketHandle.On("Object", filePath).Return(objectHandle)
			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				Client: client,
				Bucket: bucket,
				Prefix: prefix,
			}
			err := repo.Delete(ctx, namespaceSpec, jobName)

			assert.Equal(t, anError, err)
		})
		t.Run("should return error failed to get bucket", func(t *testing.T) {
			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "dev-team-1",
			}

			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, storage.ErrBucketNotExist)

			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				Client: client,
				Bucket: bucket,
				Prefix: prefix,
			}
			err := repo.Delete(ctx, namespaceSpec, jobName)

			assert.NotNil(t, err)
		})
	})
	t.Run("GetByName", func(t *testing.T) {
		bucket := "scheduled-bucket"
		prefix := "resources/jobs"
		suffix := ".py"

		exampleJob := models.Job{
			Name:     "job-1",
			Contents: []byte("content 1"),
		}
		t.Run("should read and return job object", func(t *testing.T) {
			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, nil)

			or := new(objectReaderMock)
			defer or.AssertExpectations(t)

			src := bytes.NewBuffer(exampleJob.Contents)
			mockReader := new(mockrc)
			defer mockReader.AssertExpectations(t)
			mockReader.On("Read").Return(src, nil)
			mockReader.On("Close").Return(nil)

			filePath := fmt.Sprintf("%s/%s%s", prefix, exampleJob.Name, suffix)
			or.On("NewReader", bucket, filePath).Return(mockReader, nil)

			objectHandle := new(objectHandleMock)
			defer objectHandle.AssertExpectations(t)
			objectHandle.On("Attrs", context.Background()).Return(&storage.ObjectAttrs{}, nil)

			bucketHandle.On("Object", filePath).Return(objectHandle)
			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				ObjectReader: or,
				Client:       client,
				Bucket:       bucket,
				Prefix:       prefix,
				Suffix:       suffix,
			}
			result, err := repo.GetByName(ctx, exampleJob.Name)

			assert.Nil(t, err)
			assert.Equal(t, exampleJob, result)
		})
		t.Run("should return ErrNoSuchDAG when job is not exist", func(t *testing.T) {
			nonExistentDAGName := "faulty-job"

			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			or := new(objectReaderMock)
			defer or.AssertExpectations(t)

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, nil)

			filePath := fmt.Sprintf("%s/%s%s", prefix, nonExistentDAGName, suffix)
			objectHandle := new(objectHandleMock)
			defer objectHandle.AssertExpectations(t)
			objectHandle.On("Attrs", context.Background()).Return(&storage.ObjectAttrs{}, storage.ErrObjectNotExist)

			bucketHandle.On("Object", filePath).Return(objectHandle)

			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				ObjectReader: or,
				Client:       client,
				Bucket:       bucket,
				Prefix:       prefix,
				Suffix:       suffix,
			}
			_, err := repo.GetByName(ctx, nonExistentDAGName)

			assert.Error(t, models.ErrNoSuchJob, err)
		})
		t.Run("should return error when failed to get the bucket", func(t *testing.T) {
			expected := errors.New("failed to get bucket attrs")
			or := new(objectReaderMock)
			defer or.AssertExpectations(t)

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, expected)

			client := new(storageClientMock)
			defer client.AssertExpectations(t)
			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				ObjectReader: or,
				Client:       client,
				Bucket:       bucket,
				Prefix:       prefix,
				Suffix:       suffix,
			}
			_, err := repo.GetByName(ctx, "random-job")
			assert.Equal(t, expected, err)
		})
		t.Run("should return error when failed to get object info", func(t *testing.T) {
			anotherError := errors.New("another error")
			nonExistentDAGName := "faulty-job"

			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			or := new(objectReaderMock)
			defer or.AssertExpectations(t)

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, nil)

			filePath := fmt.Sprintf("%s/%s%s", prefix, nonExistentDAGName, suffix)
			objectHandle := new(objectHandleMock)
			defer objectHandle.AssertExpectations(t)
			objectHandle.On("Attrs", context.Background()).Return(&storage.ObjectAttrs{}, anotherError)

			bucketHandle.On("Object", filePath).Return(objectHandle)

			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				ObjectReader: or,
				Client:       client,
				Bucket:       bucket,
				Prefix:       prefix,
				Suffix:       suffix,
			}
			_, err := repo.GetByName(ctx, nonExistentDAGName)

			assert.Equal(t, anotherError, err)
		})
		t.Run("should return error when job name is empty", func(t *testing.T) {
			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			or := new(objectReaderMock)
			defer or.AssertExpectations(t)

			repo := &gcsStore.JobRepository{
				Client:       client,
				ObjectReader: or,
			}
			_, err := repo.GetByName(ctx, "")
			assert.NotNil(t, err)
		})
		t.Run("should return error when to get reader", func(t *testing.T) {
			anotherError := errors.New("error getting reader")
			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, nil)

			or := new(objectReaderMock)
			defer or.AssertExpectations(t)

			filePath := fmt.Sprintf("%s/%s%s", prefix, exampleJob.Name, suffix)
			or.On("NewReader", bucket, filePath).Return(new(mockrc), anotherError)

			objectHandle := new(objectHandleMock)
			defer objectHandle.AssertExpectations(t)
			objectHandle.On("Attrs", context.Background()).Return(&storage.ObjectAttrs{}, nil)

			bucketHandle.On("Object", filePath).Return(objectHandle)
			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				ObjectReader: or,
				Client:       client,
				Bucket:       bucket,
				Prefix:       prefix,
				Suffix:       suffix,
			}
			_, err := repo.GetByName(ctx, exampleJob.Name)

			assert.Equal(t, anotherError, err)
		})
		t.Run("should return error when failed to read the object", func(t *testing.T) {
			anotherError := errors.New("error reading object")
			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, nil)

			or := new(objectReaderMock)
			defer or.AssertExpectations(t)

			src := bytes.NewBuffer(exampleJob.Contents)
			mockReader := new(mockrc)
			defer mockReader.AssertExpectations(t)
			mockReader.On("Read").Return(src, anotherError)
			mockReader.On("Close").Return(nil)

			filePath := fmt.Sprintf("%s/%s%s", prefix, exampleJob.Name, suffix)
			or.On("NewReader", bucket, filePath).Return(mockReader, nil)

			objectHandle := new(objectHandleMock)
			defer objectHandle.AssertExpectations(t)
			objectHandle.On("Attrs", context.Background()).Return(&storage.ObjectAttrs{}, nil)

			bucketHandle.On("Object", filePath).Return(objectHandle)
			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				ObjectReader: or,
				Client:       client,
				Bucket:       bucket,
				Prefix:       prefix,
				Suffix:       suffix,
			}
			_, err := repo.GetByName(ctx, exampleJob.Name)

			assert.Equal(t, anotherError, err)
		})
	})
	t.Run("GetAll", func(t *testing.T) {
		bucket := "scheduled-bucket"
		prefix := "resources/jobs"
		suffix := ".py"

		jobs := []models.Job{
			{
				Name:     "job-1",
				Contents: []byte("content 1"),
			},
			{
				Name:     "job-2",
				Contents: []byte("content 2"),
			},
		}
		t.Run("should get list of files and return the content of the files", func(t *testing.T) {
			ow := new(mocked.ObjectWriter)
			defer ow.AssertExpectations(t)

			or := new(objectReaderMock)
			defer or.AssertExpectations(t)

			for _, job := range jobs {
				src := bytes.NewBuffer(job.Contents)

				mockReader := new(mockrc)
				defer mockReader.AssertExpectations(t)
				mockReader.On("Read").Return(src, nil)
				mockReader.On("Close").Return(nil)

				or.On("NewReader", bucket, fmt.Sprintf("%s/%s%s", prefix, job.Name, suffix)).Return(mockReader, nil)
			}

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, nil)

			query := storage.Query{
				Prefix: prefix,
			}
			var objAttrs []*storage.ObjectAttrs
			for _, job := range jobs {
				objName := fmt.Sprintf("%s/%s%s", prefix, job.Name, suffix)
				objAttrs = append(objAttrs, &storage.ObjectAttrs{
					Name:        objName,
					ContentType: "text/plain; charset=utf-8",
				})
			}

			objIterator := newObjectIteratorMock(objAttrs)
			for _, objAttr := range objAttrs {
				objIterator.On("Next").Return(objAttr)
			}
			bucketHandle.On("Objects", context.Background(), &query).Return(objIterator)

			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				ObjectReader: or,
				ObjectWriter: ow,
				Client:       client,
				Bucket:       bucket,
				Prefix:       prefix,
				Suffix:       suffix,
			}
			result, err := repo.GetAll(ctx)

			assert.Nil(t, err)
			assert.Equal(t, jobs, result)
		})
		t.Run("should return error when failed to get bucket", func(t *testing.T) {
			expected := errors.New("failed to get bucket attrs")

			ow := new(mocked.ObjectWriter)
			or := new(objectReaderMock)

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, expected)

			client := new(storageClientMock)
			defer client.AssertExpectations(t)
			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				ObjectReader: or,
				ObjectWriter: ow,
				Client:       client,
				Bucket:       bucket,
				Prefix:       prefix,
			}
			_, err := repo.GetAll(ctx)
			assert.Equal(t, expected, err)
		})
		t.Run("should return error when failed to get list of the files", func(t *testing.T) {
			expected := errors.New("an error")

			ow := new(mocked.ObjectWriter)
			defer ow.AssertExpectations(t)

			or := new(objectReaderMock)
			defer or.AssertExpectations(t)

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, nil)

			query := storage.Query{
				Prefix: prefix,
			}
			objIterator := newErroneousObjectIteratorMock(expected)
			bucketHandle.On("Objects", context.Background(), &query).Return(objIterator)

			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				ObjectReader: or,
				ObjectWriter: ow,
				Client:       client,
				Bucket:       bucket,
				Prefix:       prefix,
			}
			_, err := repo.GetAll(ctx)
			assert.Equal(t, expected, err)
		})
		t.Run("should return error when failed to get the reader of a file", func(t *testing.T) {
			jobName := "faulty-job"

			getReaderError := errors.New("failed to get reader")

			ow := new(mocked.ObjectWriter)
			or := new(objectReaderMock)
			defer or.AssertExpectations(t)

			or.On("NewReader", bucket, fmt.Sprintf("%s/%s", prefix, jobName)).Return(new(mockrc), getReaderError)

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, nil)

			query := storage.Query{
				Prefix: prefix,
			}
			var objAttrs []*storage.ObjectAttrs
			objAttrs = append(objAttrs, &storage.ObjectAttrs{
				Name:        fmt.Sprintf("%s/%s", prefix, jobName),
				ContentType: "text/plain; charset=utf-8",
			})
			objIterator := newObjectIteratorMock(objAttrs)
			objIterator.On("Next").Return(objAttrs[0])
			bucketHandle.On("Objects", context.Background(), &query).Return(objIterator)

			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				ObjectReader: or,
				ObjectWriter: ow,
				Client:       client,
				Bucket:       bucket,
				Prefix:       prefix,
			}
			_, err := repo.GetAll(ctx)

			assert.Equal(t, err, getReaderError)
		})
		t.Run("should return error when failed to read a file", func(t *testing.T) {
			jobName := "faulty-job"

			ow := new(mocked.ObjectWriter)
			or := new(objectReaderMock)
			defer or.AssertExpectations(t)

			mockReader := new(mockrc)
			defer mockReader.AssertExpectations(t)
			mockReader.On("Read").Return(new(badReader), nil)
			mockReader.On("Close").Return(nil)

			or.On("NewReader", bucket, fmt.Sprintf("%s/%s.py", prefix, jobName)).Return(mockReader, nil)

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, nil)

			query := storage.Query{
				Prefix: prefix,
			}
			var objAttrs []*storage.ObjectAttrs
			objAttrs = append(objAttrs, &storage.ObjectAttrs{
				Name:        fmt.Sprintf("%s/%s%s", prefix, jobName, suffix),
				ContentType: "text/plain; charset=utf-8",
			})
			objIterator := newObjectIteratorMock(objAttrs)
			objIterator.On("Next").Return(objAttrs[0])
			bucketHandle.On("Objects", context.Background(), &query).Return(objIterator)

			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				ObjectReader: or,
				ObjectWriter: ow,
				Client:       client,
				Bucket:       bucket,
				Prefix:       prefix,
				Suffix:       suffix,
			}
			_, err := repo.GetAll(ctx)

			assert.NotNil(t, err)
		})
		t.Run("should exclude folder and only return files", func(t *testing.T) {
			ow := new(mocked.ObjectWriter)
			defer ow.AssertExpectations(t)

			or := new(objectReaderMock)
			defer or.AssertExpectations(t)

			for _, job := range jobs {
				src := bytes.NewBuffer(job.Contents)

				mockReader := new(mockrc)
				defer mockReader.AssertExpectations(t)
				mockReader.On("Read").Return(src, nil)
				mockReader.On("Close").Return(nil)

				or.On("NewReader", bucket, fmt.Sprintf("%s/%s%s", prefix, job.Name, suffix)).Return(mockReader, nil)
			}

			bucketHandle := new(storageBucketMock)
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Attrs", context.Background()).Return(&storage.BucketAttrs{}, nil)

			query := storage.Query{
				Prefix: prefix,
			}
			var objAttrs []*storage.ObjectAttrs
			for _, job := range jobs {
				objName := fmt.Sprintf("%s/%s%s", prefix, job.Name, suffix)
				objAttrs = append(objAttrs, &storage.ObjectAttrs{
					Name:        objName,
					ContentType: "text/plain; charset=utf-8",
				})
			}

			// add entry for the synthetic folder object as well
			objAttrs = append(objAttrs, &storage.ObjectAttrs{
				Name:        prefix,
				ContentType: "text/plain; charset=utf-8",
			})

			// add entry for the synthetic folder object as well
			objAttrs = append(objAttrs, &storage.ObjectAttrs{
				Name:        prefix,
				ContentType: "text/plain; charset=utf-8",
			})

			objIterator := newObjectIteratorMock(objAttrs)
			for _, objAttr := range objAttrs {
				objIterator.On("Next").Return(objAttr)
			}
			bucketHandle.On("Objects", context.Background(), &query).Return(objIterator)

			client := new(storageClientMock)
			defer client.AssertExpectations(t)

			client.On("Bucket", bucket).Return(bucketHandle)

			repo := &gcsStore.JobRepository{
				ObjectReader: or,
				ObjectWriter: ow,
				Client:       client,
				Bucket:       bucket,
				Prefix:       prefix,
				Suffix:       suffix,
			}
			result, err := repo.GetAll(ctx)

			assert.Nil(t, err)
			assert.Equal(t, jobs, result)
		})
	})
}
