package bigquery

import (
	"fmt"

	"github.com/odpf/optimus/models"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	ExternalTableTypeGoogleSheets ExternalTableType = "GOOGLE_SHEETS"
)

type ExternalTableType string

type ExternalTable struct{}

// BQExternalSource specifies table source information for external data source
type BQExternalSource struct {
	SourceType string `yaml:"type,omitempty" structs:"type"`

	// External Table URI string for the referenced spreadsheets
	SourceURIs []string `yaml:"uris,omitempty" structs:"uris,omitempty"`

	// Additional configs for CSV, GoogleSheets, Bigtable, and Parquet formats.
	Config map[string]interface{} `yaml:"config,omitempty" structs:"config"`
}

type externalTableSpec struct{}

func (s externalTableSpec) Adapter() models.DatastoreSpecAdapter {
	return &tableSpecHandler{}

}

func (s externalTableSpec) Validator() models.DatastoreSpecValidator {
	return func(spec models.ResourceSpec) error {
		if !tableNameParseRegex.MatchString(spec.Name) {
			return fmt.Errorf("for example 'project_name.dataset_name.table_name'")
		}
		parsedNames := tableNameParseRegex.FindStringSubmatch(spec.Name)
		if len(parsedNames) < 3 || len(parsedNames[1]) == 0 || len(parsedNames[2]) == 0 || len(parsedNames[3]) == 0 {
			return fmt.Errorf("for example 'project_name.dataset_name.table_name'")
		}
		return nil
	}
}

func (s externalTableSpec) DefaultAssets() map[string]string {
	return map[string]string{}
}

func extractTableSourceFromProtoStruct(protoVal *structpb.Value) *BQExternalSource {
	sInfo := &BQExternalSource{}
	if protoVal.GetStructValue() == nil {
		return sInfo
	}
	if f, ok := protoVal.GetStructValue().Fields["type"]; ok {
		sInfo.SourceType = f.GetStringValue()
	}
	if f, ok := protoVal.GetStructValue().Fields["uris"]; ok {
		var sourceURIs []string
		if f.GetListValue() != nil {
			for _, value := range f.GetListValue().GetValues() {
				sourceURIs = append(sourceURIs, value.GetStringValue())
			}
		}
		sInfo.SourceURIs = sourceURIs
	}
	if f, ok := protoVal.GetStructValue().Fields["config"]; ok {
		sInfo.Config = f.GetStructValue().AsMap()
	}
	return sInfo
}
