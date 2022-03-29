package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/models"
)

type BqClientMock struct {
	mock.Mock
	bqiface.Client
}

func (cli *BqClientMock) Location() string {
	panic("not implemented")
}

func (cli *BqClientMock) SetLocation(string) {
	panic("not implemented")
}

func (cli *BqClientMock) Close() error {
	panic("not implemented")
}

func (cli *BqClientMock) Dataset(dataset string) bqiface.Dataset {
	panic("not implemented")
}

func (cli *BqClientMock) DatasetInProject(project, dataset string) bqiface.Dataset {
	return cli.Called(project, dataset).Get(0).(bqiface.Dataset)
}

func (cli *BqClientMock) Datasets(context.Context) bqiface.DatasetIterator {
	panic("not implemented")
}

func (cli *BqClientMock) DatasetsInProject(context.Context, string) bqiface.DatasetIterator {
	panic("not implemented")
}

func (cli *BqClientMock) Query(q string) bqiface.Query {
	panic("not implemented")
}

func (cli *BqClientMock) JobFromID(context.Context, string) (bqiface.Job, error) {
	panic("not implemented")
}

func (cli *BqClientMock) JobFromIDLocation(context.Context, string, string) (bqiface.Job, error) {
	panic("not implemented")
}

func (cli *BqClientMock) Jobs(context.Context) bqiface.JobIterator {
	panic("not implemented")
}

type BqDatasetMock struct {
	mock.Mock
	bqiface.Dataset
}

func (ds *BqDatasetMock) ProjectID() string {
	panic("not implemented")
}

func (ds *BqDatasetMock) DatasetID() string {
	return ds.Called().Get(0).(string)
}

func (ds *BqDatasetMock) Create(ctx context.Context, meta *bqiface.DatasetMetadata) error {
	return ds.Called(ctx, meta).Error(0)
}

func (ds *BqDatasetMock) Delete(ctx context.Context) error {
	return ds.Called(ctx).Error(0)
}

func (ds *BqDatasetMock) DeleteWithContents(context.Context) error {
	panic("not implemented")
}

func (ds *BqDatasetMock) Metadata(ctx context.Context) (*bqiface.DatasetMetadata, error) {
	args := ds.Called(ctx)
	return args.Get(0).(*bqiface.DatasetMetadata), args.Error(1)
}

func (ds *BqDatasetMock) Update(ctx context.Context, m bqiface.DatasetMetadataToUpdate, tag string) (*bqiface.DatasetMetadata, error) {
	args := ds.Called(ctx, m, tag)
	return args.Get(0).(*bqiface.DatasetMetadata), args.Error(1)
}

func (ds *BqDatasetMock) Table(name string) bqiface.Table {
	return ds.Called(name).Get(0).(bqiface.Table)
}

func (ds *BqDatasetMock) Tables(context.Context) bqiface.TableIterator {
	panic("not implemented")
}

type BqTableMock struct {
	mock.Mock
	bqiface.Table
}

func (table *BqTableMock) CopierFrom(t ...bqiface.Table) bqiface.Copier {
	args := table.Called(t)
	return args.Get(0).(bqiface.Copier)
}

func (table *BqTableMock) Create(ctx context.Context, meta *bigquery.TableMetadata) error {
	return table.Called(ctx, meta).Error(0)
}

func (table *BqTableMock) DatasetID() string {
	panic("not implemented")
}

func (table *BqTableMock) Delete(ctx context.Context) error {
	return table.Called(ctx).Error(0)
}

func (table *BqTableMock) ExtractorTo(dst *bigquery.GCSReference) bqiface.Extractor {
	panic("not implemented")
}

func (table *BqTableMock) FullyQualifiedName() string {
	panic("not implemented")
}

func (table *BqTableMock) LoaderFrom(bigquery.LoadSource) bqiface.Loader {
	panic("not implemented")
}

func (table *BqTableMock) Metadata(ctx context.Context) (*bigquery.TableMetadata, error) {
	args := table.Called(ctx)
	return args.Get(0).(*bigquery.TableMetadata), args.Error(1)
}

func (table *BqTableMock) ProjectID() string {
	panic("not implemented")
}

func (table *BqTableMock) Read(ctx context.Context) bqiface.RowIterator {
	panic("not implemented")
}

func (table *BqTableMock) TableID() string {
	panic("not implemented")
}

func (table *BqTableMock) Update(ctx context.Context, meta bigquery.TableMetadataToUpdate, etag string) (*bigquery.TableMetadata, error) {
	args := table.Called(ctx, meta, etag)
	return args.Get(0).(*bigquery.TableMetadata), args.Error(1)
}

func (table *BqTableMock) Uploader() bqiface.Uploader {
	panic("not implemented")
}

type BQClientFactoryMock struct {
	mock.Mock
}

func (fac *BQClientFactoryMock) New(ctx context.Context, svcAcc string) (bqiface.Client, error) {
	args := fac.Called(ctx, svcAcc)
	return args.Get(0).(bqiface.Client), args.Error(1)
}

type BigQueryMock struct {
	mock.Mock
}

func (b *BigQueryMock) CreateResource(ctx context.Context, request models.CreateResourceRequest) error {
	panic("not implemented")
}

func (b *BigQueryMock) UpdateResource(ctx context.Context, request models.UpdateResourceRequest) error {
	panic("not implemented")
}

func (b *BigQueryMock) ReadResource(ctx context.Context, request models.ReadResourceRequest) (models.ReadResourceResponse, error) {
	panic("not implemented")
}

func (b *BigQueryMock) DeleteResource(ctx context.Context, request models.DeleteResourceRequest) error {
	panic("not implemented")
}

type BqCopierMock struct {
	mock.Mock
	bqiface.Copier
}

func (copier *BqCopierMock) JobIDConfig() *bigquery.JobIDConfig {
	panic("not implemented")
}

func (copier *BqCopierMock) SetCopyConfig(c bqiface.CopyConfig) {
	panic("not implemented")
}

func (copier *BqCopierMock) Run(ctx context.Context) (bqiface.Job, error) {
	args := copier.Called(ctx)
	return args.Get(0).(bqiface.Job), args.Error(1)
}

type BqJobMock struct {
	mock.Mock
	bqiface.Job
}

func (job *BqJobMock) ID() string {
	panic("not implemented")
}

func (job *BqJobMock) Location() string {
	panic("not implemented")
}

func (job *BqJobMock) Config() (bigquery.JobConfig, error) {
	panic("not implemented")
}

func (job *BqJobMock) Status(ctx context.Context) (*bigquery.JobStatus, error) {
	panic("not implemented")
}

func (job *BqJobMock) LastStatus() *bigquery.JobStatus {
	panic("not implemented")
}

func (job *BqJobMock) Cancel(ctx context.Context) error {
	panic("not implemented")
}

func (job *BqJobMock) Wait(ctx context.Context) (*bigquery.JobStatus, error) {
	args := job.Called(ctx)
	return args.Get(0).(*bigquery.JobStatus), args.Error(1)
}

func (job *BqJobMock) Read(ctx context.Context) (bqiface.RowIterator, error) {
	panic("not implemented")
}
