package bigquery

import (
	"context"
	"net/http"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"

	"github.com/raystack/optimus/core/resource"
	"github.com/raystack/optimus/internal/errors"
)

const (
	expirationTimeKey = "expiration_time"

	skipLeadingRowsKey = "skip_leading_rows"
	rangeKey           = "range"
)

type ExternalTableHandle struct {
	bqExternalTable BqTable
}

func (et ExternalTableHandle) Create(ctx context.Context, res *resource.Resource) error {
	externalTable, err := ConvertSpecTo[ExternalTable](res)
	if err != nil {
		return err
	}

	meta, err := getMetadataToCreate(externalTable.Description, externalTable.ExtraConfig, res.Metadata().Labels)
	if err != nil {
		return errors.AddErrContext(err, EntityExternalTable, "failed to get metadata to create for "+res.FullName())
	}

	if len(externalTable.Schema) > 0 {
		meta.Schema = toBQSchema(externalTable.Schema)
	}

	meta.ExternalDataConfig, err = bqExternalDataConfigTo(externalTable.Source, externalTable.Schema)
	if err != nil {
		return err
	}

	err = et.bqExternalTable.Create(ctx, meta)
	if err != nil {
		var metaErr *googleapi.Error
		if errors.As(err, &metaErr) &&
			metaErr.Code == 409 && strings.Contains(metaErr.Message, "Already Exists") {
			return errors.AlreadyExists(EntityExternalTable, "external table already exists on bigquery: "+res.FullName())
		}
		return errors.InternalError(EntityExternalTable, "failed to create external table "+res.FullName(), err)
	}
	return nil
}

func (et ExternalTableHandle) Update(ctx context.Context, res *resource.Resource) error {
	externalTable, err := ConvertSpecTo[ExternalTable](res)
	if err != nil {
		return err
	}

	meta, err := getMetadataToUpdate(externalTable.Description, externalTable.ExtraConfig, res.Metadata().Labels)
	if err != nil {
		return errors.AddErrContext(err, EntityExternalTable, "failed to get metadata to update for "+res.FullName())
	}

	if len(externalTable.Schema) > 0 {
		meta.Schema = toBQSchema(externalTable.Schema)
	}

	meta.ExternalDataConfig, err = bqExternalDataConfigTo(externalTable.Source, externalTable.Schema)
	if err != nil {
		return err
	}

	_, err = et.bqExternalTable.Update(ctx, meta, "")
	if err != nil {
		var metaErr *googleapi.Error
		if errors.As(err, &metaErr) && metaErr.Code == http.StatusNotFound {
			return errors.NotFound(EntityExternalTable, "failed to update external_table in bigquery for "+res.FullName())
		}
		return errors.InternalError(EntityExternalTable, "failed to update external_table on bigquery for "+res.FullName(), err)
	}

	return nil
}

func (et ExternalTableHandle) Exists(ctx context.Context) bool {
	_, err := et.bqExternalTable.Metadata(ctx, bigquery.WithMetadataView(bigquery.BasicMetadataView))
	// There can be connection issue, we return false for now
	return err == nil
}

func NewExternalTableHandle(bq BqTable) *ExternalTableHandle {
	return &ExternalTableHandle{bqExternalTable: bq}
}

func bqExternalDataConfigTo(es *ExternalSource, schema Schema) (*bigquery.ExternalDataConfig, error) {
	var option bigquery.ExternalDataConfigOptions
	var sourceType bigquery.DataFormat
	switch bigquery.DataFormat(strings.ToUpper(es.SourceType)) {
	case bigquery.GoogleSheets:
		option = bqGoogleSheetsOptionsTo(es.Config)
		sourceType = bigquery.GoogleSheets
	default:
		return nil, errors.InvalidArgument(EntityExternalTable, "source format not yet implemented "+es.SourceType)
	}

	externalConfig := &bigquery.ExternalDataConfig{
		SourceFormat: sourceType,
		SourceURIs:   es.SourceURIs,
		Options:      option,
	}

	if len(schema) == 0 {
		externalConfig.AutoDetect = true
	}

	return externalConfig, nil
}

func bqGoogleSheetsOptionsTo(m map[string]any) *bigquery.GoogleSheetsOptions {
	var skipLeadingRows int64

	// grpc structpb.Struct cast numbers to float64
	rows := ConfigAs[float64](m, skipLeadingRowsKey)
	if rows > 0 {
		skipLeadingRows = int64(rows)
	}

	sheetRange := ConfigAs[string](m, rangeKey)
	return &bigquery.GoogleSheetsOptions{
		SkipLeadingRows: skipLeadingRows,
		Range:           sheetRange,
	}
}

func toBQSchema(schema Schema) bigquery.Schema {
	var rv bigquery.Schema
	for _, field := range schema {
		s := &bigquery.FieldSchema{
			Name:        field.Name,
			Description: field.Description,
			Type:        bigquery.FieldType(strings.ToUpper(field.Type)),
			Required:    strings.EqualFold(ModeRequired, field.Mode),
			Repeated:    strings.EqualFold(ModeRepeated, field.Mode),
		}
		if len(field.Schema) > 0 {
			s.Schema = toBQSchema(field.Schema)
		}
		rv = append(rv, s)
	}
	return rv
}
