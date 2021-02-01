package airflow

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

const (
	baseTemplateFilePath = "./templates/scheduler/airflow_1/base_dag.py"
	baseLibFilePath      = "./templates/scheduler/airflow_1/__lib.py"
)

type scheduler struct {
	objWriter  store.ObjectWriter
	templateFS http.FileSystem
}

func NewScheduler(lfs http.FileSystem, ow store.ObjectWriter) *scheduler {
	return &scheduler{
		templateFS: lfs,
		objWriter:  ow,
	}
}

func (a *scheduler) GetName() string {
	return "airflow"
}

func (a *scheduler) GetJobsDir() string {
	return "dags"
}

func (a *scheduler) GetJobsExtension() string {
	return ".py"
}

func (a *scheduler) GetTemplatePath() string {
	return baseTemplateFilePath
}

func (a *scheduler) Bootstrap(ctx context.Context, proj models.ProjectSpec) error {
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

func (a *scheduler) migrateLibFileInGCS(ctx context.Context, bucket, objDir string) (err error) {

	// copy lib file to GCS
	baseLibFile, err := a.templateFS.Open(baseLibFilePath)
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
	dst, err := a.objWriter.NewWriter(ctx, bucket, objDir)
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
