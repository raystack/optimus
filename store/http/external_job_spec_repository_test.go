//go:build !unit_test
// +build !unit_test

package http_test

import (
	"testing"
)

func TestExternalJobSpecRepository(t *testing.T) {
	t.Run("GetJobSpecifications", func(t *testing.T) {
		t.Run("should return nil and error when host ", func(t *testing.T) {})
		t.Run("should able to return job specifications using project and job name", func(t *testing.T) {})
		t.Run("should able to return job specifications using resource destination", func(t *testing.T) {})
		//make sure in the host there is no /, either we handle it or set the convention in the docs.
	})
}
