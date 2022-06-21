package models_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/models"
)

type ResourceSpecTestSuite struct {
	suite.Suite
}

func (r *ResourceSpecTestSuite) TestEqual() {
	r.Run("should return false if version is not the same", func() {
		currentSpec := models.ResourceSpec{
			Version: 1,
		}
		incomingSpec := models.ResourceSpec{
			Version: 2,
		}

		actualValue := currentSpec.Equal(incomingSpec)

		r.False(actualValue)
	})

	r.Run("should return false if name is not the same", func() {
		currentSpec := models.ResourceSpec{
			Name: "resource1",
		}
		incomingSpec := models.ResourceSpec{
			Name: "resource2",
		}

		actualValue := currentSpec.Equal(incomingSpec)

		r.False(actualValue)
	})

	r.Run("should return false if type is not the same", func() {
		currentSpec := models.ResourceSpec{
			Type: models.ResourceTypeExternalTable,
		}
		incomingSpec := models.ResourceSpec{
			Type: models.ResourceTypeDataset,
		}

		actualValue := currentSpec.Equal(incomingSpec)

		r.False(actualValue)
	})

	r.Run("should return false if spec is not the same", func() {
		currentSpec := models.ResourceSpec{
			Spec: "spec1",
		}
		incomingSpec := models.ResourceSpec{
			Spec: "spec2",
		}

		actualValue := currentSpec.Equal(incomingSpec)

		r.False(actualValue)
	})

	r.Run("should return false if assets is not the same", func() {
		currentSpec := models.ResourceSpec{
			Assets: map[string]string{
				"key1": "value1",
			},
		}
		incomingSpec := models.ResourceSpec{
			Assets: map[string]string{
				"key2": "value2",
			},
		}

		actualValue := currentSpec.Equal(incomingSpec)

		r.False(actualValue)
	})

	r.Run("should return false if labels is not the same", func() {
		currentSpec := models.ResourceSpec{
			Labels: map[string]string{
				"key1": "value1",
			},
		}
		incomingSpec := models.ResourceSpec{
			Labels: map[string]string{
				"key2": "value2",
			},
		}

		actualValue := currentSpec.Equal(incomingSpec)

		r.False(actualValue)
	})

	r.Run("should return true if no difference on unignored fields", func() {
		currentSpec := models.ResourceSpec{
			Version: 1,
			Name:    "resource1",
			Type:    models.ResourceTypeTable,
			Spec: map[string]string{
				"key1": "value1",
			},
			Labels: map[string]string{
				"key2": "value2",
			},
		}
		incomingSpec := models.ResourceSpec{
			Version: 1,
			Name:    "resource1",
			Type:    models.ResourceTypeTable,
			Spec: map[string]string{
				"key1": "value1",
			},
			Labels: map[string]string{
				"key2": "value2",
			},
		}

		actualValue := currentSpec.Equal(incomingSpec)

		r.True(actualValue)
	})
}

func TestResourceSpec(t *testing.T) {
	suite.Run(t, &ResourceSpecTestSuite{})
}
