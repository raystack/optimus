package bigquery

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	v1 "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/models"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"gopkg.in/yaml.v3"
)

var (
	validProjectName = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`)
	validDatasetName = regexp.MustCompile(`^[\w]{3,1000}`) // golang's regex engine only let's you restrict maximum repetitions to 1000 ¯\_(ツ)_/¯
	validTableName   = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
)

const (
	ExpectedTableNameSegments = 4
	tableURNFormat            = "%s://%s:%s.%s"
)

// TableResourceSpec is how resource will be represented in yaml
type TableResourceSpec struct {
	Version int
	Name    string
	Type    models.ResourceType
	Spec    BQTableMetadata
	Labels  map[string]string
}

// BQTable is a specification for a BigQuery Table
// The table may or may not exist
//
// persistent model of table
type BQTable struct {
	Project string
	Dataset string
	Table   string

	Metadata BQTableMetadata
}

// FullyQualifiedName returns the "full name" for a table
func (t BQTable) FullyQualifiedName() string {
	return fmt.Sprintf("%s:%s.%s", t.Project, t.Dataset, t.Table)
}

func (t BQTable) Validate() error {
	switch {
	case validProjectName.MatchString(t.Project) == false:
		return fmt.Errorf("invalid project name (must match %q)", validProjectName.String())
	case validDatasetName.MatchString(t.Dataset) == false:
		return fmt.Errorf("invalid dataset name (must match %q)", validDatasetName.String())
	case validTableName.MatchString(t.Table) == false:
		return fmt.Errorf("invalid table name (must match %q)", validTableName.String())
	}
	return nil
}

// BQTableMetadata holds configuration for a table
type BQTableMetadata struct {
	Schema         BQSchema          `yaml:"schema" json:"schema"`
	Description    string            `yaml:",omitempty" json:"description,omitempty"`
	Cluster        *BQClusteringInfo `yaml:",omitempty" json:"cluster,omitempty"`
	Partition      *BQPartitionInfo  `yaml:",omitempty" json:"partition,omitempty"`
	ExpirationTime string            `yaml:"expiration_time,omitempty" json:"expiration_time,omitempty"`

	// external source options
	Source *BQExternalSource `yaml:",omitempty" json:"source,omitempty"`

	// regular view query
	ViewQuery string `yaml:"view_query,omitempty" json:"view_query,omitempty"`

	Location string            `yaml:",omitempty" json:"location,omitempty"`
	Labels   map[string]string `yaml:"-" json:"-"` // inherited
}

// BQField describes an individual field/column in a bigquery schema
type BQField struct {
	Name        string `yaml:",omitempty" json:"name"`
	Type        string `yaml:",omitempty" json:"type"`
	Description string `yaml:",omitempty" json:"description,omitempty"`
	Mode        string `yaml:",omitempty" json:"mode,omitempty"`

	// optional sub-schema, if Type is set to Record
	Schema BQSchema `yaml:",omitempty" json:"schema,omitempty"`
}

// BQSchema describes the schema for a field in a BigQuery table
type BQSchema []BQField

// BQClusteringInfo describes list of column used in table clustering
type BQClusteringInfo struct {
	Using []string `json:"using"`
}

// BQPartitionInfo specifies the partitioning for a BQTable
type BQPartitionInfo struct {
	Field string `yaml:"field,omitempty" json:"field,omitempty"`

	// time based
	Type       string `yaml:"type,omitempty" json:"type,omitempty"`             // default day
	Expiration int64  `yaml:"expiration,omitempty" json:"expiration,omitempty"` // in hours

	// range based
	Range *BQPartitioningRange `yaml:",omitempty" json:"range,omitempty"`
}

// BQPartitioningRange defines the boundaries and width of partitioned values.
type BQPartitioningRange struct {
	// The start value of defined range of values, inclusive of the specified value.
	Start int64 `yaml:",omitempty" json:"start,omitempty"`
	// The end of the defined range of values, exclusive of the defined value.
	End int64 `yaml:",omitempty" json:"end,omitempty"`
	// The width of each interval range.
	Interval int64 `yaml:",omitempty" json:"interval,omitempty"`
}

// tableSpecHandler helps serializing/deserializing datastore resource for table
type tableSpecHandler struct {
}

func (s tableSpecHandler) ToYaml(optResource models.ResourceSpec) ([]byte, error) {
	if optResource.Spec == nil {
		// usually happens when resource is requested to be created for the first time via optimus cli
		optResource.Spec = BQTable{}
	}
	spec, ok := optResource.Spec.(BQTable)
	if !ok {
		return nil, errors.New("failed to convert resource, malformed spec")
	}

	yamlResource := TableResourceSpec{
		Version: optResource.Version,
		Name:    optResource.Name,
		Type:    optResource.Type,
		Spec:    spec.Metadata,
	}
	if len(yamlResource.Labels) > 0 {
		yamlResource.Labels = optResource.Labels
	}
	return yaml.Marshal(yamlResource)
}

func (s tableSpecHandler) FromYaml(b []byte) (models.ResourceSpec, error) {
	var yamlResource TableResourceSpec
	if err := yaml.Unmarshal(b, &yamlResource); err != nil {
		return models.ResourceSpec{}, err
	}

	parsedTableName := tableNameParseRegex.FindStringSubmatch(yamlResource.Name)
	if len(parsedTableName) < ExpectedTableNameSegments {
		return models.ResourceSpec{}, fmt.Errorf("invalid yamlResource name %s", yamlResource.Name)
	}

	optResource := models.ResourceSpec{
		Version:   yamlResource.Version,
		Name:      yamlResource.Name,
		Type:      yamlResource.Type,
		Datastore: This,
		Spec: BQTable{
			Project:  parsedTableName[1],
			Dataset:  parsedTableName[2],
			Table:    parsedTableName[3],
			Metadata: yamlResource.Spec,
		},
	}

	if len(yamlResource.Labels) > 0 {
		optResource.Labels = yamlResource.Labels
	}
	return optResource, nil
}

func (s tableSpecHandler) ToProtobuf(optResource models.ResourceSpec) ([]byte, error) {
	bqResource, ok := optResource.Spec.(BQTable)
	if !ok {
		return nil, errors.New("failed to convert resource, malformed spec")
	}
	bqResourceProtoSpec, err := convertToStructPB(bqResource.Metadata)
	if err != nil {
		return nil, err
	}
	resSpec := &v1.ResourceSpecification{
		Version: int32(optResource.Version),
		Name:    optResource.Name,
		Type:    optResource.Type.String(),
		Spec:    bqResourceProtoSpec,
		Assets:  optResource.Assets,
		Labels:  optResource.Labels,
	}
	return proto.Marshal(resSpec)
}

func (s tableSpecHandler) FromProtobuf(b []byte) (models.ResourceSpec, error) {
	protoSpec := &v1.ResourceSpecification{}
	if err := proto.Unmarshal(b, protoSpec); err != nil {
		return models.ResourceSpec{}, err
	}

	parsedTableName := tableNameParseRegex.FindStringSubmatch(protoSpec.Name)
	if len(parsedTableName) < ExpectedTableNameSegments {
		return models.ResourceSpec{}, fmt.Errorf("invalid resource name %s", protoSpec.Name)
	}

	bqTable := BQTable{
		Project: parsedTableName[1],
		Dataset: parsedTableName[2],
		Table:   parsedTableName[3],
	}
	if protoSpec.Spec != nil {
		var tableSchema BQSchema
		if protoSpecField, ok := protoSpec.Spec.Fields["schema"]; ok {
			tableSchema = extractTableSchemaFromProtoStruct(protoSpecField)
		}

		var description string
		if protoSpecField, ok := protoSpec.Spec.Fields["description"]; ok {
			description = strings.TrimSpace(protoSpecField.GetStringValue())
		}

		var location string
		if protoSpecField, ok := protoSpec.Spec.Fields["location"]; ok {
			location = strings.TrimSpace(protoSpecField.GetStringValue())
		}

		var viewQuery string
		if protoSpecField, ok := protoSpec.Spec.Fields["view_query"]; ok {
			viewQuery = protoSpecField.GetStringValue()
		}

		var externalSource *BQExternalSource
		if protoSpecField, ok := protoSpec.Spec.Fields["source"]; ok {
			externalSource = extractTableSourceFromProtoStruct(protoSpecField)
		}

		bqTable.Metadata = BQTableMetadata{
			Schema:      tableSchema,
			Description: description,
			ViewQuery:   viewQuery,
			Location:    location,
			Source:      externalSource,
		}

		if protoSpecField, ok := protoSpec.Spec.Fields["expiration_time"]; ok {
			bqTable.Metadata.ExpirationTime = strings.TrimSpace(protoSpecField.GetStringValue())
		}

		if protoSpecField, ok := protoSpec.Spec.Fields["cluster"]; ok {
			clusteringInfo := &BQClusteringInfo{
				Using: []string{},
			}
			clusterStruct := protoSpecField.GetStructValue()
			if clusterStruct != nil {
				if clusterUsing, ok := clusterStruct.Fields["using"]; ok {
					if clusterUsing.GetListValue() != nil {
						for _, usingValue := range clusterUsing.GetListValue().GetValues() {
							clusteringInfo.Using = append(clusteringInfo.Using, usingValue.GetStringValue())
						}
					}
				}
			}
			bqTable.Metadata.Cluster = clusteringInfo
		}

		if protoSpecField, ok := protoSpec.Spec.Fields["partition"]; ok {
			bqTable.Metadata.Partition = extractTablePartitionFromProtoStruct(protoSpecField)
		}
	}
	return models.ResourceSpec{
		Version:   int(protoSpec.Version),
		Name:      protoSpec.Name,
		Type:      models.ResourceType(protoSpec.Type),
		Assets:    protoSpec.Assets,
		Spec:      bqTable,
		Datastore: This,
		Labels:    protoSpec.Labels,
	}, nil
}

func extractTableSchemaFromProtoStruct(val *structpb.Value) BQSchema {
	bqSchema := BQSchema{}
	if val.GetListValue() == nil {
		return nil
	}
	for _, schemaListValues := range val.GetListValue().GetValues() {
		bqSchema = append(bqSchema, extractTableSchemaFieldFromProto(schemaListValues))
	}
	return bqSchema
}

func extractTableSchemaFieldFromProto(schemaListValues *structpb.Value) BQField {
	bqField := BQField{}
	for schemaAttr, schemaAttrVal := range schemaListValues.GetStructValue().Fields {
		switch schemaAttr {
		case "name":
			bqField.Name = strings.TrimSpace(schemaAttrVal.GetStringValue())
		case "type":
			bqField.Type = strings.TrimSpace(schemaAttrVal.GetStringValue())
		case "description":
			bqField.Description = strings.TrimSpace(schemaAttrVal.GetStringValue())
		case "mode":
			bqField.Mode = strings.TrimSpace(schemaAttrVal.GetStringValue())
		case "schema":
			bqField.Schema = extractTableSchemaFromProtoStruct(schemaAttrVal)
		}
	}
	return bqField
}

func extractTablePartitionFromProtoStruct(protoVal *structpb.Value) *BQPartitionInfo {
	pInfo := &BQPartitionInfo{}
	if protoVal.GetStructValue() == nil {
		return pInfo
	}
	if f, ok := protoVal.GetStructValue().Fields["field"]; ok {
		pInfo.Field = strings.TrimSpace(f.GetStringValue())
	}
	if f, ok := protoVal.GetStructValue().Fields["type"]; ok {
		pInfo.Type = strings.TrimSpace(f.GetStringValue())
	}
	if f, ok := protoVal.GetStructValue().Fields["expiration"]; ok {
		pInfo.Expiration = int64(f.GetNumberValue())
	}
	if f, ok := protoVal.GetStructValue().Fields["range"]; ok {
		pRange := &BQPartitioningRange{}
		if startV, ok := f.GetStructValue().Fields["start"]; ok {
			pRange.Start = int64(startV.GetNumberValue())
		}
		if endV, ok := f.GetStructValue().Fields["end"]; ok {
			pRange.End = int64(endV.GetNumberValue())
		}
		if intervalV, ok := f.GetStructValue().Fields["interval"]; ok {
			pRange.Interval = int64(intervalV.GetNumberValue())
		}
		pInfo.Range = pRange
	}
	return pInfo
}

type tableSpec struct{}

func (s tableSpec) Adapter() models.DatastoreSpecAdapter {
	return &tableSpecHandler{}
}

func (s tableSpec) Validator() models.DatastoreSpecValidator {
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

func (s tableSpec) GenerateURN(tableConfig interface{}) (string, error) {
	bqTable, ok := tableConfig.(BQTable)
	if !ok {
		return "", errors.New("failed to read table spec for bigquery")
	}
	return fmt.Sprintf(tableURNFormat, BigQuery{}.Name(), bqTable.Project, bqTable.Dataset, bqTable.Table), nil
}

func (s tableSpec) DefaultAssets() map[string]string {
	return map[string]string{}
}

func convertToStructPB(val interface{}) (*structpb.Struct, error) {
	var mapGeneric map[string]interface{}
	rawBytes, err := json.Marshal(val)
	if err != nil {
		return nil, fmt.Errorf("json.Marshal: %v: %w", val, err)
	}
	if err := json.Unmarshal(rawBytes, &mapGeneric); err != nil {
		return nil, fmt.Errorf("json.Unmarshal: %v: %w", mapGeneric, err)
	}
	protoStruct, err := structpb.NewStruct(mapGeneric)
	if err != nil {
		return nil, fmt.Errorf("structpb.NewStruct(): %v: %w", mapGeneric, err)
	}
	return protoStruct, nil
}
