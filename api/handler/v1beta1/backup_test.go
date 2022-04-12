package v1beta1_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

func TestBackupOnServer(t *testing.T) {
	log := log.NewNoop()
	ctx := context.Background()

	t.Run("BackupDryRun", func(t *testing.T) {
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			ID:   models.ProjectID(uuid.New()),
			Name: projectName,
		}
		namespaceSpec := models.NamespaceSpec{
			ID:   uuid.New(),
			Name: "dev-test-namespace-1",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
			ProjectSpec: projectSpec,
		}
		resourceName := "a-data-project:dataset.table"
		resourceUrn := "bigquery://a-data-project:dataset.table"

		t.Run("should return list of resources for backup ignoring downstream", func(t *testing.T) {
			jobName := "a-data-job"
			jobSpec := models.JobSpec{
				ID:   uuid.New(),
				Name: jobName,
				Task: models.JobSpecTask{
					Config: models.JobSpecConfigs{
						{
							Name:  "do",
							Value: "this",
						},
					},
				},
				Assets: *models.JobAssets{}.New(
					[]models.JobSpecAsset{
						{
							Name:  "query.sql",
							Value: "select * from 1",
						},
					}),
			}
			jobSpecDownstreams := []models.JobSpec{
				{
					ID:   uuid.New(),
					Name: "b-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
			}

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)
			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			jobService.On("GetDownstream", ctx, projectSpec, jobSpec.Name).Return(jobSpecDownstreams, nil)

			backupRequest := models.BackupRequest{
				ResourceName: resourceName,
				Project:      projectSpec,
				Namespace:    namespaceSpec,
				DryRun:       true,
			}
			backupPlan := models.BackupPlan{Resources: []string{resourceName}}
			resourceSvc.On("BackupResourceDryRun", ctx, backupRequest, []models.JobSpec{jobSpec, jobSpecDownstreams[0]}).Return(backupPlan, nil)

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				jobService,
				resourceSvc,
				namespaceService,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
			}
			backupResponse, err := backupServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, []string{resourceName}, backupResponse.ResourceName)
		})
		t.Run("should return list of resources for backup with downstream", func(t *testing.T) {
			jobSpec := models.JobSpec{
				ID:   uuid.New(),
				Name: "a-data-job",
			}
			jobSpecDownstreams := []models.JobSpec{
				{
					ID:   uuid.New(),
					Name: "b-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
				{
					ID:   uuid.New(),
					Name: "c-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
			}
			allowedDownstream := []string{models.AllNamespace}

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}

			resourceDownstream1Urn := "bigquery://a-data-project:dataset.downstream1"
			resourceDownstream2Urn := "bigquery://a-data-project:dataset.downstream2"

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)
			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			jobService.On("GetDownstream", ctx, projectSpec, jobSpec.Name).Return(jobSpecDownstreams, nil)

			backupRequest := models.BackupRequest{
				ResourceName:                resourceName,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: allowedDownstream,
			}
			backupPlan := models.BackupPlan{
				Resources: []string{
					resourceUrn,
					resourceDownstream1Urn,
					resourceDownstream2Urn,
				},
			}
			resourceSvc.On("BackupResourceDryRun", ctx, backupRequest,
				[]models.JobSpec{jobSpec, jobSpecDownstreams[0], jobSpecDownstreams[1]}).Return(backupPlan, nil)

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				jobService,
				resourceSvc,
				namespaceService,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:                 projectName,
				DatastoreName:               models.DestinationTypeBigquery.String(),
				ResourceName:                resourceName,
				NamespaceName:               namespaceSpec.Name,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			backupResponse, err := backupServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, []string{resourceUrn, resourceDownstream1Urn, resourceDownstream2Urn}, backupResponse.ResourceName)
		})
		t.Run("should return list of resources for backup with same namespace downstream", func(t *testing.T) {
			jobSpec := models.JobSpec{
				ID:   uuid.New(),
				Name: "a-data-job",
			}
			jobSpecDownstreams := []models.JobSpec{
				{
					ID:   uuid.New(),
					Name: "b-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
				{
					ID:   uuid.New(),
					Name: "c-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
			}

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}

			resourceDownstream1Urn := "bigquery://a-data-project:dataset.downstream1"
			resourceDownstream2Urn := "bigquery://a-data-project:dataset.downstream2"

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)
			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			jobService.On("GetDownstream", ctx, projectSpec, jobSpec.Name).Return(jobSpecDownstreams, nil)

			backupRequest := models.BackupRequest{
				ResourceName:                resourceName,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			backupPlan := models.BackupPlan{
				Resources: []string{
					resourceUrn,
					resourceDownstream1Urn,
					resourceDownstream2Urn,
				},
			}
			resourceSvc.On("BackupResourceDryRun", ctx, backupRequest,
				[]models.JobSpec{jobSpec, jobSpecDownstreams[0], jobSpecDownstreams[1]}).Return(backupPlan, nil)

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				jobService,
				resourceSvc,
				namespaceService,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:                 projectName,
				DatastoreName:               models.DestinationTypeBigquery.String(),
				ResourceName:                resourceName,
				NamespaceName:               namespaceSpec.Name,
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			backupResponse, err := backupServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, []string{resourceUrn, resourceDownstream1Urn, resourceDownstream2Urn}, backupResponse.ResourceName)
		})
		t.Run("should return error when project is not found", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)
			errorMsg := "unable to fetch project"
			namespaceService.On("Get", ctx, projectName, "sample-namespace").
				Return(models.NamespaceSpec{}, errors.New(errorMsg))

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				nil,
				nil, namespaceService,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: "sample-namespace",
			}
			backupResponse, err := backupServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when namespace is not found", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)
			errorMsg := "unable to get namespace"
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).
				Return(models.NamespaceSpec{}, errors.New(errorMsg))

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				nil,
				nil,
				namespaceService,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
			}
			backupResponse, err := backupServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to read resource", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)
			errorMsg := "unable to read resource"
			resourceSvc.On("ReadResource", ctx, namespaceSpec,
				models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, errors.New(errorMsg))

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				nil,
				resourceSvc,
				namespaceService,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
			}
			backupResponse, err := backupServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to get jobSpec", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)
			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			errorMsg := "unable to get jobspec"
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(models.JobSpec{}, errors.New(errorMsg))

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				jobService,
				resourceSvc,
				namespaceService,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
			}
			backupResponse, err := backupServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to get jobSpec downstream", func(t *testing.T) {
			jobName := "a-data-job"
			jobSpec := models.JobSpec{
				ID:   uuid.New(),
				Name: jobName,
				Task: models.JobSpecTask{
					Config: models.JobSpecConfigs{
						{
							Name:  "do",
							Value: "this",
						},
					},
				},
				Assets: *models.JobAssets{}.New(
					[]models.JobSpecAsset{
						{
							Name:  "query.sql",
							Value: "select * from 1",
						},
					}),
			}
			allowedDownstream := []string{models.AllNamespace}

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)
			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			errorMsg := "unable to get jobspec downstream"
			jobService.On("GetDownstream", ctx, projectSpec, jobSpec.Name).Return([]models.JobSpec{}, errors.New(errorMsg))

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				jobService,
				resourceSvc,
				namespaceService,
				nil,
			)
			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:                 projectName,
				DatastoreName:               models.DestinationTypeBigquery.String(),
				ResourceName:                resourceName,
				NamespaceName:               namespaceSpec.Name,
				AllowedDownstreamNamespaces: allowedDownstream,
			}
			backupResponse, err := backupServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to do backup dry run", func(t *testing.T) {
			jobName := "a-data-job"
			jobSpec := models.JobSpec{
				ID:   uuid.New(),
				Name: jobName,
				Task: models.JobSpecTask{
					Config: models.JobSpecConfigs{
						{
							Name:  "do",
							Value: "this",
						},
					},
				},
				Assets: *models.JobAssets{}.New(
					[]models.JobSpecAsset{
						{
							Name:  "query.sql",
							Value: "select * from 1",
						},
					}),
			}
			allowedDownstream := []string{models.AllNamespace}

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)
			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			jobService.On("GetDownstream", ctx, projectSpec, jobSpec.Name).Return([]models.JobSpec{}, nil)

			backupRequest := models.BackupRequest{
				ResourceName:                resourceName,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: allowedDownstream,
			}
			errorMsg := "unable to get jobspec"

			resourceSvc.On("BackupResourceDryRun", ctx, backupRequest, []models.JobSpec{jobSpec}).
				Return(models.BackupPlan{}, errors.New(errorMsg))

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				jobService,
				resourceSvc,
				namespaceService,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:                 projectName,
				DatastoreName:               models.DestinationTypeBigquery.String(),
				ResourceName:                resourceName,
				NamespaceName:               namespaceSpec.Name,
				AllowedDownstreamNamespaces: allowedDownstream,
			}
			backupResponse, err := backupServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
	})

	t.Run("Backup", func(t *testing.T) {
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			ID:   models.ProjectID(uuid.New()),
			Name: projectName,
		}
		namespaceSpec := models.NamespaceSpec{
			ID:   uuid.New(),
			Name: "dev-test-namespace-1",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
			ProjectSpec: projectSpec,
		}
		resourceName := "a-data-project:dataset.table"
		resourceUrn := "datastore://a-data-project:dataset.table"
		backupUrn := "datastore://a-data-project:optimus_backup.table_backup"

		t.Run("should able to do backup ignoring downstream", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			jobName := "a-data-job"
			jobSpec := models.JobSpec{
				ID:   uuid.New(),
				Name: jobName,
				Task: models.JobSpecTask{
					Config: models.JobSpecConfigs{
						{
							Name:  "do",
							Value: "this",
						},
					},
				},
				Assets: *models.JobAssets{}.New(
					[]models.JobSpecAsset{
						{
							Name:  "query.sql",
							Value: "select * from 1",
						},
					}),
			}
			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}
			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
				Config: map[string]string{
					"TTL": "30",
				},
			}
			backupReq := models.BackupRequest{
				ResourceName: resourceName,
				Project:      projectSpec,
				Namespace:    namespaceSpec,
				Config: map[string]string{
					"TTL": "30",
				},
				DryRun: false,
			}
			backupResponsePb := &pb.CreateBackupResponse{
				Urn: []string{backupUrn},
			}
			backupResult := models.BackupResult{
				Resources: []string{backupUrn},
			}

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			resourceSvc.On("BackupResource", ctx, backupReq, []models.JobSpec{jobSpec}).Return(backupResult, nil)

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				jobService,
				resourceSvc,
				namespaceService,
				nil,
			)
			backupResponse, err := backupServiceServer.CreateBackup(ctx, &backupRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, backupResponsePb, backupResponse)
		})
		t.Run("should return list of resources for backup with downstream", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			jobSpec := models.JobSpec{
				ID:   uuid.New(),
				Name: "a-data-job",
			}
			jobSpecDownstreams := []models.JobSpec{
				{
					ID:   uuid.New(),
					Name: "b-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
				{
					ID:   uuid.New(),
					Name: "c-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
			}
			allowedDownstream := []string{models.AllNamespace}
			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}
			backupDownstream1Urn := "datastore://a-data-project:optimus_backup.downstream1"
			backupDownstream2Urn := "datastore://a-data-project:optimus_backup.downstream2"
			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
				Config: map[string]string{
					"TTL": "30",
				},
				AllowedDownstreamNamespaces: allowedDownstream,
			}
			backupReq := models.BackupRequest{
				ResourceName: resourceName,
				Project:      projectSpec,
				Namespace:    namespaceSpec,
				Config: map[string]string{
					"TTL": "30",
				},
				DryRun:                      false,
				AllowedDownstreamNamespaces: allowedDownstream,
			}
			urns := []string{backupUrn, backupDownstream1Urn, backupDownstream2Urn}
			backupResult := models.BackupResult{
				Resources: urns,
			}

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			jobService.On("GetDownstream", ctx, projectSpec, jobSpec.Name).Return(jobSpecDownstreams, nil)

			resourceSvc.On("BackupResource", ctx, backupReq, []models.JobSpec{jobSpec, jobSpecDownstreams[0], jobSpecDownstreams[1]}).Return(backupResult, nil)

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				jobService,
				resourceSvc,
				namespaceService,
				nil,
			)
			backupResponse, err := backupServiceServer.CreateBackup(ctx, &backupRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, backupResult.Resources, backupResponse.Urn)
			assert.Equal(t, backupResult.IgnoredResources, backupResponse.IgnoredResources)
		})
		t.Run("should return list of resources for backup with downstream with only allowed namespace", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			jobSpec := models.JobSpec{
				ID:   uuid.New(),
				Name: "a-data-job",
			}
			jobSpecDownstreams := []models.JobSpec{
				{
					ID:   uuid.New(),
					Name: "b-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
				{
					ID:   uuid.New(),
					Name: "c-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
			}
			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}
			backupDownstream1Urn := "datastore://a-data-project:optimus_backup.downstream1"
			backupDownstream2Urn := "datastore://a-data-project:optimus_backup.downstream2"
			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
				Config: map[string]string{
					"TTL": "30",
				},
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			backupReq := models.BackupRequest{
				ResourceName: resourceName,
				Project:      projectSpec,
				Namespace:    namespaceSpec,
				Config: map[string]string{
					"TTL": "30",
				},
				DryRun:                      false,
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			urns := []string{backupUrn, backupDownstream1Urn, backupDownstream2Urn}
			backupResult := models.BackupResult{
				Resources: urns,
			}

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			jobService.On("GetDownstream", ctx, projectSpec, jobSpec.Name).Return(jobSpecDownstreams, nil)

			resourceSvc.On("BackupResource", ctx, backupReq, []models.JobSpec{jobSpec, jobSpecDownstreams[0], jobSpecDownstreams[1]}).Return(backupResult, nil)

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				jobService,
				resourceSvc,
				namespaceService,
				nil,
			)
			backupResponse, err := backupServiceServer.CreateBackup(ctx, &backupRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, backupResult.Resources, backupResponse.Urn)
			assert.Equal(t, backupResult.IgnoredResources, backupResponse.IgnoredResources)
		})
		t.Run("should return error when project is not found", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).
				Return(models.NamespaceSpec{}, errors.New("failed to get project"))

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceSpec.Name,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
			}

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				jobService,
				resourceSvc,
				namespaceService,
				nil,
			)
			backupResponse, err := backupServiceServer.CreateBackup(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), "failed to get project")
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when namespace is not found", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
			}

			errorMsg := "unable to get namespace"
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(models.NamespaceSpec{}, errors.New(errorMsg))

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				nil,
				nil,
				namespaceService,
				nil,
			)
			backupResponse, err := backupServiceServer.CreateBackup(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to read resource", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}
			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
			}

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			errorMsg := "unable to read resource"
			resourceSvc.On("ReadResource", ctx, namespaceSpec,
				models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, errors.New(errorMsg))

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				nil, resourceSvc,
				namespaceService,
				nil,
			)
			backupResponse, err := backupServiceServer.CreateBackup(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to get jobSpec", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}
			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
			}

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)
			errorMsg := "unable to get jobspec"
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(models.JobSpec{}, errors.New(errorMsg))

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				jobService,
				resourceSvc,
				namespaceService,
				nil,
			)
			backupResponse, err := backupServiceServer.CreateBackup(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to get jobSpec downstream", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			jobName := "a-data-job"
			jobSpec := models.JobSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: jobName,
				Task: models.JobSpecTask{
					Config: models.JobSpecConfigs{
						{
							Name:  "do",
							Value: "this",
						},
					},
				},
				Assets: *models.JobAssets{}.New(
					[]models.JobSpecAsset{
						{
							Name:  "query.sql",
							Value: "select * from 1",
						},
					}),
			}
			allowedDownstream := []string{models.AllNamespace}
			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}
			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:                 projectName,
				DatastoreName:               models.DestinationTypeBigquery.String(),
				ResourceName:                resourceName,
				NamespaceName:               namespaceSpec.Name,
				AllowedDownstreamNamespaces: allowedDownstream,
			}

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			errorMsg := "unable to get jobspec downstream"
			jobService.On("GetDownstream", ctx, projectSpec, jobSpec.Name).Return([]models.JobSpec{}, errors.New(errorMsg))

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				jobService,
				resourceSvc,
				namespaceService,
				nil,
			)
			backupResponse, err := backupServiceServer.CreateBackup(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to do backup", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			jobName := "a-data-job"
			jobSpec := models.JobSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: jobName,
				Task: models.JobSpecTask{
					Config: models.JobSpecConfigs{
						{
							Name:  "do",
							Value: "this",
						},
					},
				},
				Assets: *models.JobAssets{}.New(
					[]models.JobSpecAsset{
						{
							Name:  "query.sql",
							Value: "select * from 1",
						},
					}),
			}
			allowedDownstream := []string{models.AllNamespace}
			backupReq := models.BackupRequest{
				ResourceName: resourceName,
				Project:      projectSpec,
				Namespace:    namespaceSpec,
				Config: map[string]string{
					"TTL": "30",
				},
				DryRun:                      false,
				AllowedDownstreamNamespaces: allowedDownstream,
			}
			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}
			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
				Config: map[string]string{
					"TTL": "30",
				},
				AllowedDownstreamNamespaces: allowedDownstream,
			}

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			jobService.On("GetDownstream", ctx, projectSpec, jobSpec.Name).Return([]models.JobSpec{}, nil)
			errorMsg := "unable to get jobspec"
			resourceSvc.On("BackupResource", ctx, backupReq, []models.JobSpec{jobSpec}).Return(models.BackupResult{}, errors.New(errorMsg))

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				jobService,
				resourceSvc,
				namespaceService,
				nil,
			)
			backupResponse, err := backupServiceServer.CreateBackup(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
	})

	t.Run("ListBackups", func(t *testing.T) {
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			ID:   models.ProjectID(uuid.New()),
			Name: projectName,
		}
		datastoreName := models.DestinationTypeBigquery.String()
		namespaceSpec := models.NamespaceSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "dev-test-namespace-1",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
			ProjectSpec: projectSpec,
		}
		listBackupsReq := pb.ListBackupsRequest{
			ProjectName:   projectName,
			DatastoreName: datastoreName,
			NamespaceName: namespaceSpec.Name,
		}
		backupSpecs := []models.BackupSpec{
			{
				ID:        uuid.Must(uuid.NewRandom()),
				CreatedAt: time.Now().Add(time.Hour * 24 * -30),
				Resource: models.ResourceSpec{
					Name: "sample resource",
				},
				Description: "backup purpose",
			},
			{
				ID:        uuid.Must(uuid.NewRandom()),
				CreatedAt: time.Now().Add(time.Hour * 24 * -50),
				Resource: models.ResourceSpec{
					Name: "sample resource",
				},
				Description: "backup purpose",
			},
		}
		t.Run("should return list of backups", func(t *testing.T) {
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			backupResultPb := &pb.ListBackupsResponse{
				Backups: []*pb.BackupSpec{
					{
						Id:           backupSpecs[0].ID.String(),
						ResourceName: backupSpecs[0].Resource.Name,
						CreatedAt:    timestamppb.New(backupSpecs[0].CreatedAt),
						Description:  backupSpecs[0].Description,
					},
					{
						Id:           backupSpecs[1].ID.String(),
						ResourceName: backupSpecs[1].Resource.Name,
						CreatedAt:    timestamppb.New(backupSpecs[1].CreatedAt),
						Description:  backupSpecs[1].Description,
					},
				},
			}

			resourceSvc.On("ListResourceBackups", ctx, projectSpec, datastoreName).Return(backupSpecs, nil)

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				nil, resourceSvc,
				nil,
				projectService,
			)
			backupResponse, err := backupServiceServer.ListBackups(ctx, &listBackupsReq)

			assert.Nil(t, err)
			assert.Equal(t, backupResultPb, backupResponse)
		})
		t.Run("should return error when unable to get project spec", func(t *testing.T) {
			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			errorMsg := "unable to get project spec"
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).
				Return(models.ProjectSpec{}, errors.New(errorMsg))
			defer projectService.AssertExpectations(t)

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				nil, resourceSvc,
				nil,
				projectService,
			)
			backupResponse, err := backupServiceServer.ListBackups(ctx, &listBackupsReq)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to get list of backups", func(t *testing.T) {
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			errorMsg := "unable to get list of backups"
			resourceSvc.On("ListResourceBackups", ctx, projectSpec, datastoreName).Return([]models.BackupSpec{}, errors.New(errorMsg))

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				nil, resourceSvc,
				nil,
				projectService,
			)
			backupResponse, err := backupServiceServer.ListBackups(ctx, &listBackupsReq)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
	})

	t.Run("GetBackup", func(t *testing.T) {
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			ID:   models.ProjectID(uuid.New()),
			Name: projectName,
		}
		datastoreName := models.DestinationTypeBigquery.String()
		namespaceSpec := models.NamespaceSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "dev-test-namespace-1",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
			ProjectSpec: projectSpec,
		}
		backupID := uuid.Must(uuid.NewRandom())
		getBackupDetailReq := pb.GetBackupRequest{
			ProjectName:   projectName,
			DatastoreName: datastoreName,
			NamespaceName: namespaceSpec.Name,
			Id:            backupID.String(),
		}
		backupSpec := models.BackupSpec{
			ID:        backupID,
			CreatedAt: time.Now().Add(time.Hour * 24 * -30),
			Resource: models.ResourceSpec{
				Name: "sample resource",
			},
			Description: "backup purpose",
		}
		t.Run("should return backup detail", func(t *testing.T) {
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			backupResultPb := &pb.GetBackupResponse{
				Spec: &pb.BackupSpec{
					Id:           backupSpec.ID.String(),
					ResourceName: backupSpec.Resource.Name,
					CreatedAt:    timestamppb.New(backupSpec.CreatedAt),
					Description:  backupSpec.Description,
				},
			}

			resourceSvc.On("GetResourceBackup", ctx, projectSpec, datastoreName, backupID).Return(backupSpec, nil)

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				nil, resourceSvc,
				nil,
				projectService,
			)
			backupResponse, err := backupServiceServer.GetBackup(ctx, &getBackupDetailReq)

			assert.Nil(t, err)
			assert.Equal(t, backupResultPb, backupResponse)
		})
		t.Run("should return error when unable to get project spec", func(t *testing.T) {
			errorMsg := "unable to get project spec"
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, errors.New(errorMsg))
			defer projectService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				nil, resourceSvc,
				nil,
				projectService,
			)
			backupResponse, err := backupServiceServer.GetBackup(ctx, &getBackupDetailReq)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to get backup detail", func(t *testing.T) {
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			errorMsg := "unable to get backup detail"
			resourceSvc.On("GetResourceBackup", ctx, projectSpec, datastoreName, backupID).Return(models.BackupSpec{}, errors.New(errorMsg))

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				nil, resourceSvc,
				nil,
				projectService,
			)
			backupResponse, err := backupServiceServer.GetBackup(ctx, &getBackupDetailReq)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when backup is not found", func(t *testing.T) {
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)
			resourceSvc.On("GetResourceBackup", ctx, projectSpec, datastoreName, backupID).Return(models.BackupSpec{}, store.ErrResourceNotFound)

			errorMsg := fmt.Sprintf("backup with ID %s not found", backupID)
			backupServiceServer := v1.NewBackupServiceServer(
				log,
				nil, resourceSvc,
				nil,
				projectService,
			)
			backupResponse, err := backupServiceServer.GetBackup(ctx, &getBackupDetailReq)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when backup URN is not found", func(t *testing.T) {
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			invalidBackupSpec := models.BackupSpec{
				ID:        backupID,
				CreatedAt: time.Now().Add(time.Hour * 24 * -30),
				Resource: models.ResourceSpec{
					Name: "sample resource",
				},
				Description: "backup purpose",
				Result: map[string]interface{}{
					"sample_table": map[string]interface{}{
						"other_key": "other_value",
					},
				},
			}

			resourceSvc.On("GetResourceBackup", ctx, projectSpec, datastoreName, backupID).Return(invalidBackupSpec, nil)

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				nil, resourceSvc,
				nil,
				projectService,
			)
			backupResponse, err := backupServiceServer.GetBackup(ctx, &getBackupDetailReq)

			assert.Nil(t, backupResponse)
			assert.Contains(t, err.Error(), "URN is not found in backup result")
		})
		t.Run("should return error when backup URN is invalid", func(t *testing.T) {
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			invalidBackupSpec := models.BackupSpec{
				ID:        backupID,
				CreatedAt: time.Now().Add(time.Hour * 24 * -30),
				Resource: models.ResourceSpec{
					Name: "sample resource",
				},
				Description: "backup purpose",
				Result: map[string]interface{}{
					"sample_table": map[string]interface{}{
						models.BackupSpecKeyURN: 0,
					},
				},
			}
			resourceSvc.On("GetResourceBackup", ctx, projectSpec, datastoreName, backupID).Return(invalidBackupSpec, nil)

			backupServiceServer := v1.NewBackupServiceServer(
				log,
				nil, resourceSvc,
				nil,
				projectService,
			)
			backupResponse, err := backupServiceServer.GetBackup(ctx, &getBackupDetailReq)

			assert.Nil(t, backupResponse)
			assert.Contains(t, err.Error(), "invalid backup URN")
		})
	})
}
