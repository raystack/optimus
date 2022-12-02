package resource

import (
	"github.com/odpf/optimus/internal/errors"
)

type Kind string

const (
	KindDataset       Kind = "dataset"
	KindTable         Kind = "table"
	KindView          Kind = "view"
	KindExternalTable Kind = "external_table"
)

func (k Kind) String() string {
	return string(k)
}

func FromStringToKind(name string) (Kind, error) {
	switch name {
	case string(KindDataset):
		return KindDataset, nil
	case string(KindTable):
		return KindTable, nil
	case string(KindView):
		return KindView, nil
	case string(KindExternalTable):
		return KindExternalTable, nil
	default:
		return "", errors.InvalidArgument(EntityResource, "unknown kind "+name)
	}
}

type Metadata struct {
	Version     int32
	Description string
	Labels      map[string]string
}
