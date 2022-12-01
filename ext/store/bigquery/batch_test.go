package bigquery_test

import (
	"context"
	"testing"

	"github.com/kushsharma/parallel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/store/bigquery"
	"github.com/odpf/optimus/internal/errors"
)

func TestBatches(t *testing.T) {
	ctx := context.Background()
	meta1 := &resource.Metadata{Version: 1}
	spec := map[string]any{
		"description": "test resource",
	}
	kindDS := resource.KindDataset
	store := resource.Bigquery

	tnnt, _ := tenant.NewTenant("proj", "ns")
	ds1Name := "t-optimus.playground"
	ds2Name := "t-optimus.mart"

	ds1, resErr := resource.NewResource(ds1Name, kindDS, store, tnnt, meta1, spec)
	assert.Nil(t, resErr)
	tab1, resErr := resource.NewResource(ds1Name+".table1", resource.KindTable, store, tnnt, meta1, spec)
	assert.Nil(t, resErr)
	view1, resErr := resource.NewResource(ds1Name+".view1", resource.KindView, store, tnnt, meta1, spec)
	assert.Nil(t, resErr)

	extTab1, resErr := resource.NewResource(ds2Name+".extTable1", resource.KindExternalTable, store, tnnt, meta1, spec)
	assert.Nil(t, resErr)
	view2, resErr := resource.NewResource(ds2Name+".view2", resource.KindView, store, tnnt, meta1, spec)
	assert.Nil(t, resErr)

	t.Run("BatchesFrom", func(t *testing.T) {
		invalidRes, err2 := resource.NewResource(ds2Name+".invalid1", "stream", store, tnnt, meta1, spec)
		assert.Nil(t, err2)

		batches := bigquery.BatchesFrom([]*resource.Resource{ds1, tab1, view1, extTab1, view2, invalidRes}, nil)
		assert.Equal(t, 2, len(batches))

		batch1, ok := batches[ds1Name]
		assert.True(t, ok)
		assert.NotNil(t, batch1)
		assert.NotNil(t, batch1.DatasetDetails)
		assert.Equal(t, 1, len(batch1.Tables))
		assert.Equal(t, 1, len(batch1.Views))

		batch2, ok := batches[ds2Name]
		assert.True(t, ok)
		assert.NotNil(t, batch2)
		assert.Nil(t, batch2.DatasetDetails)
		assert.Equal(t, 1, len(batch2.ExternalTables))
		assert.Equal(t, 1, len(batch2.Views))
	})
	t.Run("return error when getting client fails", func(t *testing.T) {
		clientProvider := new(mockClientProvider)
		clientProvider.On("Get", ctx, "secret_value").
			Return(nil, errors.InvalidArgument("client", "cannot create"))

		batches := bigquery.BatchesFrom([]*resource.Resource{tab1}, clientProvider)
		batch1 := batches[tab1.Dataset().FullName()]

		err := batch1.QueueJobs(ctx, "secret_value", nil)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity client: cannot create")
	})
	t.Run("queues the jobs for execution", func(t *testing.T) {
		updateDS := resource.FromExisting(ds1, resource.ReplaceStatus(resource.StatusToUpdate))

		createTab1 := resource.FromExisting(tab1, resource.ReplaceStatus(resource.StatusToCreate))
		updateTab1 := resource.FromExisting(tab1, resource.ReplaceStatus(resource.StatusToUpdate))

		createView1 := resource.FromExisting(view1, resource.ReplaceStatus(resource.StatusToCreate))
		updateView1 := resource.FromExisting(view1, resource.ReplaceStatus(resource.StatusToUpdate))

		createExt1 := resource.FromExisting(extTab1, resource.ReplaceStatus(resource.StatusToCreate))
		updateExt1 := resource.FromExisting(extTab1, resource.ReplaceStatus(resource.StatusToUpdate))

		createView2 := resource.FromExisting(view2, resource.ReplaceStatus(resource.StatusToCreate))
		updateView2 := resource.FromExisting(view2, resource.ReplaceStatus(resource.StatusToUpdate))

		tab2, resErr := resource.NewResource(ds1Name+".table2", resource.KindTable, store, tnnt, meta1, spec)
		assert.Nil(t, resErr)
		successTab2 := resource.FromExisting(tab2, resource.ReplaceStatus(resource.StatusSuccess))

		datasetHandle := new(mockTableResourceHandle)
		datasetHandle.On("Exists", ctx).Return(true)
		datasetHandle.On("Update", ctx, mock.Anything).Return(nil)
		defer datasetHandle.AssertExpectations(t)

		tableHandle := new(mockTableResourceHandle)
		tableHandle.On("Exists", ctx).Return(false)
		tableHandle.On("Create", ctx, createTab1).Return(nil)
		tableHandle.On("Update", ctx, updateTab1).Return(nil)
		defer tableHandle.AssertExpectations(t)

		viewHandle := new(mockTableResourceHandle)
		viewHandle.On("Exists", ctx).Return(false)
		viewHandle.On("Create", ctx, createView1).Return(errors.InternalError("view1", "some err", nil))
		viewHandle.On("Update", ctx, updateView1).Return(nil)
		viewHandle.On("Create", ctx, createView2).Return(errors.AlreadyExists("view2", "already"))
		viewHandle.On("Update", ctx, updateView2).Return(nil)
		defer viewHandle.AssertExpectations(t)

		extTableHandle := new(mockTableResourceHandle)
		extTableHandle.On("Update", ctx, updateExt1).Return(errors.InternalError("ext1", "err", nil))
		extTableHandle.On("Exists", ctx).Return(true)
		defer extTableHandle.AssertExpectations(t)

		client := new(mockClient)
		client.On("DatasetHandleFrom", updateDS.Dataset()).Return(datasetHandle)
		client.On("TableHandleFrom", createTab1.Dataset(), createTab1.Name()).Return(tableHandle)
		client.On("TableHandleFrom", updateTab1.Dataset(), updateTab1.Name()).Return(tableHandle)
		client.On("TableHandleFrom", successTab2.Dataset(), successTab2.Name()).Return(tableHandle)
		client.On("ViewHandleFrom", createView1.Dataset(), createView1.Name()).Return(viewHandle)
		client.On("ViewHandleFrom", updateView1.Dataset(), updateView1.Name()).Return(viewHandle)
		client.On("ViewHandleFrom", createView2.Dataset(), createView2.Name()).Return(viewHandle)
		client.On("ViewHandleFrom", updateView2.Dataset(), updateView2.Name()).Return(viewHandle)
		client.On("ExternalTableHandleFrom", createExt1.Dataset(), createExt1.Name()).Return(extTableHandle)
		client.On("ExternalTableHandleFrom", updateExt1.Dataset(), updateExt1.Name()).Return(extTableHandle)
		bth := bigquery.Batch{Dataset: updateExt1.Dataset()}
		ds, _ := bth.DatasetOrDefault()
		client.On("DatasetHandleFrom", ds.Dataset()).Return(datasetHandle)
		defer client.AssertExpectations(t)

		clientProvider := new(mockClientProvider)
		clientProvider.On("Get", ctx, "secret_value").Return(client, nil)
		defer clientProvider.AssertExpectations(t)

		batches := bigquery.BatchesFrom([]*resource.Resource{
			updateDS, createTab1, createView1, createExt1, createView2, successTab2,
			updateTab1, updateView1, updateExt1, updateView2,
		}, clientProvider)

		testParallel := parallel.NewRunner()
		for _, batch := range batches {
			err := batch.QueueJobs(ctx, "secret_value", testParallel)
			assert.Nil(t, err)
		}

		states := testParallel.RunSerial()

		assert.Equal(t, resource.StatusSuccess, updateDS.Status())
		assert.Equal(t, resource.StatusSuccess, createTab1.Status())
		assert.Equal(t, resource.StatusCreateFailure, createView1.Status())
		assert.Equal(t, resource.StatusSuccess, createExt1.Status())
		assert.Equal(t, resource.StatusSuccess, createView2.Status())
		assert.Equal(t, resource.StatusSuccess, successTab2.Status())
		assert.Equal(t, resource.StatusSuccess, updateTab1.Status())
		assert.Equal(t, resource.StatusSuccess, updateView1.Status())
		assert.Equal(t, resource.StatusUpdateFailure, updateExt1.Status())
		assert.Equal(t, resource.StatusSuccess, updateView2.Status())

		positions := []int{0, 1, 2, 3, 5, 6, 7, 9, 10}
		for _, i := range positions {
			assert.Nil(t, states[i].Err)
		}

		assert.NotNil(t, states[4].Err)
		assert.EqualError(t, states[4].Err, "internal error for entity view1: some err")

		// Dataset for batch2
		dataset2, ok := states[6].Val.(*resource.Resource)
		assert.True(t, ok)
		assert.Equal(t, resource.StatusSuccess, dataset2.Status())

		extErr := states[8].Err
		assert.NotNil(t, extErr)
		assert.EqualError(t, extErr, "internal error for entity ext1: err")
	})
}
