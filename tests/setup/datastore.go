package setup

import (
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/ext/datastore/bigquery"
)

func MockBigQueryDataStore() *bigquery.BigQuery {
	bQClient := new(bigquery.BqClientMock)

	bQClientFactory := new(bigquery.BQClientFactoryMock)
	bQClientFactory.On("New", mock.Anything, mock.Anything).Return(bQClient, nil)

	return &bigquery.BigQuery{
		ClientFac: bQClientFactory,
	}
}
