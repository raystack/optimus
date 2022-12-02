package bigquery

import (
	"fmt"
	"regexp"

	"github.com/mitchellh/mapstructure"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/internal/errors"
)

const (
	EntityDataset = "dataset"

	DatesetNameSections = 2
	TableNameSections   = 3
)

var (
	validProjectName = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`)
	validDatasetName = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	validTableName   = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
)

type DatasetDetails struct {
	Name resource.Name

	Description string                 `mapstructure:"description,omitempty"`
	ExtraConfig map[string]interface{} `mapstructure:",remain"`
}

func (d DatasetDetails) FullName() string {
	return d.Name.String()
}

func (DatasetDetails) Validate() error {
	return nil
}

func ConvertSpecTo[T DatasetDetails | Table | View | ExternalTable](res *resource.Resource) (*T, error) {
	var spec T
	if err := mapstructure.Decode(res.Spec(), &spec); err != nil {
		msg := fmt.Sprintf("%s: not able to decode spec for %s", err, res.FullName())
		return nil, errors.InvalidArgument(resource.EntityResource, msg)
	}

	return &spec, nil
}

type Dataset struct {
	Project     string
	DatasetName string
}

func DataSetFrom(project string, datasetName string) (Dataset, error) {
	if project == "" {
		return Dataset{}, errors.InvalidArgument(EntityDataset, "bigquery project name is empty")
	}

	if datasetName == "" {
		return Dataset{}, errors.InvalidArgument(EntityDataset, "bigquery dataset name is empty")
	}

	return Dataset{
		Project:     project,
		DatasetName: datasetName,
	}, nil
}

func (d Dataset) FullName() string {
	return d.Project + "." + d.DatasetName
}

func DataSetFor(res *resource.Resource) (Dataset, error) {
	sections := res.NameSections()
	if len(sections) < DatesetNameSections {
		return Dataset{}, errors.InvalidArgument(EntityDataset, "invalid dataset name: "+res.FullName())
	}

	return DataSetFrom(sections[0], sections[1])
}

func ResourceNameFor(res *resource.Resource) (string, error) {
	sections := res.NameSections()
	if res.Kind() == resource.KindDataset {
		if len(sections) < DatesetNameSections {
			return "", errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+res.FullName())
		}
		return sections[1], nil
	}

	if len(sections) < TableNameSections {
		return "", errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+res.FullName())
	}
	return sections[2], nil
}

func ValidateName(res *resource.Resource) error {
	sections := res.NameSections()
	if len(sections) < DatesetNameSections {
		return errors.InvalidArgument(resource.EntityResource, "invalid sections in name: "+res.FullName())
	}

	if !validProjectName.MatchString(sections[0]) {
		return errors.InvalidArgument(resource.EntityResource, "invalid character in project name "+res.FullName())
	}

	if !validDatasetName.MatchString(sections[1]) {
		return errors.InvalidArgument(resource.EntityResource, "invalid character in dataset name "+res.FullName())
	}

	if res.Kind() != resource.KindDataset {
		if len(sections) != TableNameSections {
			return errors.InvalidArgument(resource.EntityResource, "invalid resource name sections: "+res.FullName())
		}

		if !validTableName.MatchString(sections[2]) {
			return errors.InvalidArgument(resource.EntityResource, "invalid character in resource name "+res.FullName())
		}
	}
	return nil
}
