package bigquery

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	v1 "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/models"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

var (
	datasetNameParseRegex = regexp.MustCompile(`^([\w-]+)\.(\w+)$`)
	datasetURNFormat      = "%s://%s:%s"
)

const (
	ExpectedDatasetNameSegments = 3
)

// DatasetResourceSpec is how dataset should be represented in yaml
type DatasetResourceSpec struct {
	Version int
	Name    string
	Type    models.ResourceType
	Spec    BQDatasetMetadata
	Labels  map[string]string
}

// BQDataset is a specification for a BigQuery Dataset
// The dataset may or may not exist
type BQDataset struct {
	Project  string
	Dataset  string
	Metadata BQDatasetMetadata
}

type BQDatasetMetadata struct {
	Description            string            `yaml:",omitempty" json:"description,omitempty"`
	DefaultTableExpiration int64             `yaml:"table_expiration,omitempty" json:"table_expiration,omitempty"`
	Labels                 map[string]string `yaml:"-" json:"-"` // will be inherited by base resource

	Location string `yaml:",omitempty" json:"location,omitempty"`
}

// datasetSpecHandler helps serializing/deserializing datastore resource for dataset
type datasetSpecHandler struct {
}

func (s datasetSpecHandler) ToYaml(optResource models.ResourceSpec) ([]byte, error) {
	if optResource.Spec == nil {
		// usually happens when resource is requested to be created for the first time via optimus cli
		optResource.Spec = BQDataset{}
	}
	bqResource, ok := optResource.Spec.(BQDataset)
	if !ok {
		return nil, errors.New("failed to convert resource, malformed spec")
	}

	yamlResource := DatasetResourceSpec{
		Version: optResource.Version,
		Name:    optResource.Name,
		Type:    optResource.Type,
		Spec:    bqResource.Metadata,
		Labels:  optResource.Labels,
	}
	return yaml.Marshal(yamlResource)
}

func (s datasetSpecHandler) FromYaml(b []byte) (models.ResourceSpec, error) {
	var yamlResource DatasetResourceSpec
	if err := yaml.Unmarshal(b, &yamlResource); err != nil {
		return models.ResourceSpec{}, err
	}

	parsedNames := datasetNameParseRegex.FindStringSubmatch(yamlResource.Name)
	if len(parsedNames) < ExpectedDatasetNameSegments {
		return models.ResourceSpec{}, fmt.Errorf("invalid resource name %s", yamlResource.Name)
	}

	optResource := models.ResourceSpec{
		Version:   yamlResource.Version,
		Name:      yamlResource.Name,
		Type:      yamlResource.Type,
		Datastore: This,
		Spec: BQDataset{
			Project:  parsedNames[1],
			Dataset:  parsedNames[2],
			Metadata: yamlResource.Spec,
		},
		Labels: yamlResource.Labels,
	}
	return optResource, nil
}

func (s datasetSpecHandler) ToProtobuf(optResource models.ResourceSpec) ([]byte, error) {
	bqResource, ok := optResource.Spec.(BQDataset)
	if !ok {
		return nil, errors.New("failed to convert resource, malformed spec")
	}

	// json to and from serialization is needed to get correct map[string]interface
	bqResourceProtoSpec, err := convertToStructPB(bqResource.Metadata)
	if err != nil {
		return nil, err
	}
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

func (s datasetSpecHandler) FromProtobuf(b []byte) (models.ResourceSpec, error) {
	baseSpec := &v1.ResourceSpecification{}
	if err := proto.Unmarshal(b, baseSpec); err != nil {
		return models.ResourceSpec{}, err
	}

	parsedNames := datasetNameParseRegex.FindStringSubmatch(baseSpec.Name)
	if len(parsedNames) < ExpectedDatasetNameSegments {
		return models.ResourceSpec{}, fmt.Errorf("invalid resource name %s", baseSpec.Name)
	}

	bqMeta := BQDatasetMetadata{}
	if baseSpec.Spec != nil {
		if protoSpecField, ok := baseSpec.Spec.Fields["description"]; ok {
			bqMeta.Description = strings.TrimSpace(protoSpecField.GetStringValue())
		}

		if protoSpecField, ok := baseSpec.Spec.Fields["location"]; ok {
			bqMeta.Location = strings.TrimSpace(protoSpecField.GetStringValue())
		}

		if protoSpecField, ok := baseSpec.Spec.Fields["table_expiration"]; ok {
			bqMeta.DefaultTableExpiration = int64(protoSpecField.GetNumberValue())
		}
	}

	optResource := models.ResourceSpec{
		Version:   int(baseSpec.Version),
		Name:      baseSpec.Name,
		Type:      models.ResourceType(baseSpec.Type),
		Assets:    baseSpec.Assets,
		Datastore: This,
		Spec: BQDataset{
			Project:  parsedNames[1],
			Dataset:  parsedNames[2],
			Metadata: bqMeta,
		},
		Labels: baseSpec.Labels,
	}
	return optResource, nil
}

type datasetSpec struct{}

func (s datasetSpec) Adapter() models.DatastoreSpecAdapter {
	return &datasetSpecHandler{}
}

func (s datasetSpec) Validator() models.DatastoreSpecValidator {
	return func(spec models.ResourceSpec) error {
		if !datasetNameParseRegex.MatchString(spec.Name) {
			return fmt.Errorf("for example 'project_name.dataset_name'")
		}
		parsedNames := datasetNameParseRegex.FindStringSubmatch(spec.Name)
		if len(parsedNames) < 3 || len(parsedNames[1]) == 0 || len(parsedNames[2]) == 0 {
			return fmt.Errorf("for example 'project_name.dataset_name'")
		}
		return nil
	}
}

func (s datasetSpec) GenerateURN(tableConfig interface{}) (string, error) {
	bqDataset, ok := tableConfig.(BQDataset)
	if !ok {
		return "", errors.New("failed to read dataset spec for bigquery")
	}
	return fmt.Sprintf(datasetURNFormat, BigQuery{}.Name(), bqDataset.Project, bqDataset.Dataset), nil
}

func (s datasetSpec) DefaultAssets() map[string]string {
	return map[string]string{}
}
