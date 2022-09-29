package local_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/odpf/optimus/client/local"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type ResourceSpecTestSuite struct {
	suite.Suite
}

func TestResourceSpecTestSuite(t *testing.T) {
	suite.Run(t, &ResourceSpecReadWriterTestSuite{})
}

func (r *ResourceSpecReadWriterTestSuite) TestToProto() {
	r.Run("should return resource spec proto and nil if no error is encountered", func() {
		spec := map[string]interface{}{
			"schema": []interface{}{
				map[string]interface{}{
					"name": "id",
					"type": "string",
					"mode": "nullable",
				},
				map[string]interface{}{
					"name": "name",
					"type": "string",
					"mode": "nullable",
				},
			},
		}
		resourceSpec := &local.ResourceSpec{
			Version: 1,
			Name:    "resource",
			Type:    "table",
			Spec:    spec,
			Labels: map[string]string{
				"orchestrator": "optimus",
			},
		}

		specProto, err := structpb.NewStruct(spec)
		r.Require().NoError(err)
		expectedResourceSpecProto := &pb.ResourceSpecification{
			Version: 1,
			Name:    "resource",
			Type:    "table",
			Labels: map[string]string{
				"orchestrator": "optimus",
			},
			Spec:   specProto,
			Assets: nil,
		}

		actualResourceSpecProto, actualError := resourceSpec.ToProto()

		r.Assert().EqualValues(expectedResourceSpecProto, actualResourceSpecProto)
		r.Assert().NoError(actualError)
	})
}
