package airflow

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
)

const (
	baseTemplateFilePath = "./templates/scheduler/airflow_1/base_dag.py"
	baseLibFilePath      = "./templates/scheduler/airflow_1/__lib.py"
)

type AirflowScheduler struct {
	GcsClient  *storage.Client
	TemplateFS http.FileSystem
}

func (a *AirflowScheduler) GetName() string {
	return "airflow"
}

func (a *AirflowScheduler) GetJobsDir() string {
	return "dags"
}

func (a *AirflowScheduler) GetJobsExtension() string {
	return ".py"
}

func (a *AirflowScheduler) GetTemplatePath() string {
	return baseTemplateFilePath
}

func (a *AirflowScheduler) Bootstrap(ctx context.Context, proj models.ProjectSpec) error {
	storagePath, ok := proj.Config[models.ProjectStoragePathKey]
	if !ok {
		return errors.Errorf("%s not configured for project %s", models.ProjectStoragePathKey, proj.Name)
	}
	p, err := url.Parse(storagePath)
	if err != nil {
		return err
	}

	switch p.Scheme {
	case "gs":
		return a.migrateLibFileInGCS(ctx, p.Hostname(), filepath.Join(p.Path, a.GetJobsDir(), filepath.Base(baseLibFilePath)))
	}

	return errors.Errorf("unsupported storage config %s in %s of project %s", storagePath, models.ProjectStoragePathKey, proj.Name)
}

func (a *AirflowScheduler) migrateLibFileInGCS(ctx context.Context, bucket, objDir string) (err error) {

	// copy lib file to GCS
	baseLibFile, err := a.TemplateFS.Open(baseLibFilePath)
	if err != nil {
		return err
	}
	defer baseLibFile.Close()

	// read file
	fileContent, err := ioutil.ReadAll(baseLibFile)
	if err != nil {
		return err
	}

	// copy to gcs
	dst := a.GcsClient.Bucket(bucket).Object(objDir).NewWriter(ctx)
	defer func() {
		if derr := dst.Close(); derr != nil {
			if err == nil {
				err = derr
			} else {
				err = errors.Wrap(err, derr.Error())
			}
		}
	}()

	_, err = io.Copy(dst, bytes.NewBuffer(fileContent))
	return
}
