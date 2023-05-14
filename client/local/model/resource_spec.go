package model

import (
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type ResourceSpec struct {
	Version int                    `yaml:"version"`
	Name    string                 `yaml:"name"`
	Type    string                 `yaml:"type"`
	Labels  map[string]string      `yaml:"labels"`
	Spec    map[string]interface{} `yaml:"spec"`
}

func (r ResourceSpec) ToProto() (*pb.ResourceSpecification, error) {
	specPb, err := structpb.NewStruct(r.Spec)
	if err != nil {
		return nil, fmt.Errorf("error constructing spec pb: %w", err)
	}
	return &pb.ResourceSpecification{
		Version: int32(r.Version),
		Name:    r.Name,
		Type:    r.Type,
		Labels:  r.Labels,
		Spec:    specPb,
		Assets:  nil, // TODO: check if we really need assets
	}, nil
}
