package local_test

import (
	"fmt"
	"io/fs"
	"os"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/odpf/optimus/client/local"
	"github.com/stretchr/testify/suite"
)

type JobReaderWriterTestSuite struct {
	suite.Suite
}

func TestJobReaderWriter(t *testing.T) {
	s := new(JobReaderWriterTestSuite)
	suite.Run(t, s)
}

func (s *JobReaderWriterTestSuite) ReadAll_Success() {
	s.T().Skip() // TODO: remove once read all implementation is done
	specFS := createValidSpecFS(
		"root/ns1/jobs/example1",
		"root/ns1/jobs/example2",
		"root/ns2/jobs/example3",
		"root/ns2/jobs/example4",
	)
	jobReaderWriter, err := local.NewJobSpecReadWriter(specFS)
	s.Require().NoError(err)

	_, _ = jobReaderWriter.ReadAll("root")
	// TODO: check ReadAll result
}

func (s *JobReaderWriterTestSuite) ReadAll_Fail() {
	s.T().Skip() // TODO: remove once read all implementation is done
	s.Run("when discoverSpecDirPath error", func() {
		// TODO: implement test fail here
	})
	s.Run("when individual read spec error", func() {
		// TODO: implement test fail here
	})
}

func createValidSpecFS(specDirPaths ...string) fs.FS {
	template := `
version: 1
name: godata.ds.%s
owner: optimus@optimus.dev
schedule:
  start_date: "2022-03-22"
  interval: 0 22 * * * 
task:
  name: bq2bq
  config:
    PROJECT: godata
    DATASET: example
    TABLE: %s
    SQL_TYPE: STANDARD
    LOAD_METHOD: REPLACE
  window:
    size: 24h
    offset: 24h
    truncate_to: d
labels:
  orchestrator: optimus
`

	specFS := fstest.MapFS{}

	for _, specDirPath := range specDirPaths {
		splittedDirPath := strings.Split(specDirPath, "/")
		jobName := splittedDirPath[len(splittedDirPath)-1]

		dataRaw := fmt.Sprintf(template, jobName, jobName)
		// TODO: create dummy assets

		specFS[specDirPath] = &fstest.MapFile{
			Data: []byte(dataRaw),
			Mode: os.ModeTemporary,
		}
	}

	return specFS
}
