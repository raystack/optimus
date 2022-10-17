package bigquery

import (
	"context"
	"net/http"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/internal/errors"
)

const (
	expirationTimeKey = "expiration_time"
)

type ExternalTableHandle struct {
	bqExternalTable BqTable
}

func (et ExternalTableHandle) Create(ctx context.Context, res *resource.Resource) error {
	externalTable, err := resource.ConvertSpecTo[resource.ExternalTable](res)
	if err != nil {
		return err
	}

	meta, err := getMetadataToCreate(externalTable.Description, externalTable.ExtraConfig, res.Metadata().Labels)
	if err != nil {
		return errors.AddErrContext(err, resource.EntityExternalTable, "failed to get metadata to create for "+res.FullName())
	}

	meta.Schema = toBQSchema(externalTable.Schema)

	if externalTable.Source != nil {
		meta.ExternalDataConfig, err = bqExternalDataConfigTo(externalTable.Source)
		if err != nil {
			return err
		}
	}

	err = et.bqExternalTable.Create(ctx, meta)
	if err != nil {
		var metaErr *googleapi.Error
		if errors.As(err, &metaErr) &&
			metaErr.Code == 409 && strings.Contains(metaErr.Message, "Already Exists") {
			return errors.AlreadyExists(resource.EntityExternalTable, "external table already exists on bigquery: "+res.FullName())
		}
		return errors.InternalError(resource.EntityExternalTable, "failed to create external table "+res.FullName(), err)
	}
	return nil
}

func (et ExternalTableHandle) Update(ctx context.Context, res *resource.Resource) error {
	externalTable, err := resource.ConvertSpecTo[resource.ExternalTable](res)
	if err != nil {
		return err
	}

	meta, err := getMetadataToUpdate(externalTable.Description, externalTable.ExtraConfig, res.Metadata().Labels)
	if err != nil {
		return errors.AddErrContext(err, resource.EntityExternalTable, "failed to get metadata to update for "+res.FullName())
	}

	_, err = et.bqExternalTable.Update(ctx, meta, "")
	if err != nil {
		var metaErr *googleapi.Error
		if errors.As(err, &metaErr) && metaErr.Code == http.StatusNotFound {
			return errors.NotFound(resource.EntityExternalTable, "failed to update external_table in bigquery for "+res.FullName())
		}
		return errors.InternalError(resource.EntityExternalTable, "failed to update external_table on bigquery for "+res.FullName(), err)
	}

	return nil
}

func NewExternalTableHandle(bq BqTable) *ExternalTableHandle {
	return &ExternalTableHandle{bqExternalTable: bq}
}

func bqExternalDataConfigTo(es *resource.ExternalSource) (*bigquery.ExternalDataConfig, error) {
	var option bigquery.ExternalDataConfigOptions
	var sourceType bigquery.DataFormat
	switch bigquery.DataFormat(strings.ToUpper(es.SourceType)) {
	case bigquery.GoogleSheets:
		option = bqGoogleSheetsOptionsTo(es.Config)
		sourceType = bigquery.GoogleSheets
	default:
		return nil, errors.InvalidArgument(resource.EntityExternalTable, "source format not yet implemented "+es.SourceType)
	}

	externalConfig := &bigquery.ExternalDataConfig{
		SourceFormat: sourceType,
		SourceURIs:   es.SourceURIs,
		Options:      option,
	}
	return externalConfig, nil
}

func bqGoogleSheetsOptionsTo(m map[string]any) *bigquery.GoogleSheetsOptions {
	var skipLeadingRows int64
	var sheetRange string

	if val, ok := m["skip_leading_rows"]; ok {
		if rows, ok := val.(int); ok {
			skipLeadingRows = int64(rows)
		}
	}
	if val, ok := m["range"]; ok {
		if ran, ok := val.(string); ok {
			sheetRange = ran
		}
	}
	return &bigquery.GoogleSheetsOptions{
		SkipLeadingRows: skipLeadingRows,
		Range:           sheetRange,
	}
}

func toBQSchema(schema resource.Schema) bigquery.Schema {
	var rv bigquery.Schema
	for _, field := range schema {
		s := &bigquery.FieldSchema{
			Name:        field.Name,
			Description: field.Description,
			Type:        bigquery.FieldType(strings.ToUpper(field.Type)),
			Required:    strings.EqualFold(resource.ModeRequired, field.Mode),
			Repeated:    strings.EqualFold(resource.ModeRepeated, field.Mode),
		}
		if len(field.Schema) > 0 {
			s.Schema = toBQSchema(field.Schema)
		}
		rv = append(rv, s)
	}
	return rv
}
