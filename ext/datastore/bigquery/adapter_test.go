package bigquery

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
)

func TestAdapter(t *testing.T) {
	sampleFieldName := "fieldName"
	sampleFieldType := "STRING"
	sampleFieldDescription := "field description"
	sampleFieldMode := "required"
	sampleFieldParentName := "fieldParentName"
	sampleFieldParentType := "RECORD"
	sampleFieldParentDescription := "field parent description"
	sampleFieldParentMode := "repeated"
	sampleFieldChildName := "fieldChildName"
	sampleFieldChildType := "INTEGER"
	sampleFieldChildDescription := "field child description"
	sampleFieldChildMode := "nullable"
	schema := BQSchema{
		BQField{
			Name:        sampleFieldName,
			Type:        sampleFieldType,
			Description: sampleFieldDescription,
			Mode:        sampleFieldMode,
			Schema:      BQSchema{},
		},
		BQField{
			Name:        sampleFieldParentName,
			Type:        sampleFieldParentType,
			Description: sampleFieldParentDescription,
			Mode:        sampleFieldParentMode,
			Schema: BQSchema{
				BQField{
					Name:        sampleFieldChildName,
					Type:        sampleFieldChildType,
					Description: sampleFieldChildDescription,
					Mode:        sampleFieldChildMode,
					Schema:      BQSchema{},
				},
			},
		},
	}
	bQSchema := bigquery.Schema{
		&bigquery.FieldSchema{
			Name:        sampleFieldName,
			Description: sampleFieldDescription,
			Required:    true,
			Type:        bigquery.StringFieldType,
		},
		&bigquery.FieldSchema{
			Name:        sampleFieldParentName,
			Description: sampleFieldParentDescription,
			Repeated:    true,
			Type:        bigquery.RecordFieldType,
			Schema: bigquery.Schema{
				&bigquery.FieldSchema{
					Name:        sampleFieldChildName,
					Description: sampleFieldChildDescription,
					Type:        bigquery.IntegerFieldType,
				},
			},
		},
	}

	t.Run("should convert from and to BQ TimePartitioning successfully", func(t *testing.T) {
		partitionField := "partition-field"
		partitionExpiryInHours := int64(720)
		partitionInfo := BQPartitionInfo{
			Field:      partitionField,
			Type:       "DAY",
			Expiration: partitionExpiryInHours,
		}
		expectedBQTimePartitioning := &bigquery.TimePartitioning{
			Type:       bigquery.DayPartitioningType,
			Expiration: time.Duration(partitionExpiryInHours) * time.Hour,
			Field:      partitionField,
		}
		bQTimePartitioningResult := bqPartitioningTimeTo(partitionInfo)
		assert.Equal(t, expectedBQTimePartitioning, bQTimePartitioningResult)

		partitionInfoResult := bqPartitioningFrom(bQTimePartitioningResult)
		assert.Equal(t, &partitionInfo, partitionInfoResult)
	})
	t.Run("should convert from and to BQ TimePartitioning Range successfully", func(t *testing.T) {
		partitionField := "partition-field"
		rangeStart := int64(0)
		rangeEnd := int64(100)
		rangeInterval := int64(10)
		partitionInfo := BQPartitionInfo{
			Field: partitionField,
			Range: &BQPartitioningRange{
				Start:    rangeStart,
				End:      rangeEnd,
				Interval: rangeInterval,
			},
		}
		expectedBQRangePartitioning := &bigquery.RangePartitioning{
			Field: partitionField,
			Range: &bigquery.RangePartitioningRange{
				Start:    rangeStart,
				End:      rangeEnd,
				Interval: rangeInterval,
			},
		}
		bQRangePartitioningResult := bqPartitioningRangeTo(partitionInfo)
		assert.Equal(t, expectedBQRangePartitioning, bQRangePartitioningResult)

		partitionInfoResult := bqPartitioningRangeFrom(bQRangePartitioningResult.Range)
		assert.Equal(t, partitionInfo.Range, partitionInfoResult)
	})
	t.Run("should convert from and to BQ Clustering successfully", func(t *testing.T) {
		clusteringFields := []string{"clustering-field"}
		clusteringInfo := &BQClusteringInfo{
			Using: clusteringFields,
		}
		expectedBQClustering := &bigquery.Clustering{
			Fields: clusteringFields,
		}

		bQClusteringResult := bqClusteringTo(clusteringInfo)
		assert.Equal(t, expectedBQClustering, bQClusteringResult)

		clusteringInfoResult := bqClusteringFrom(bQClusteringResult)
		assert.Equal(t, clusteringInfo, clusteringInfoResult)
	})
	t.Run("should convert from and to BQ Field Mode successfully", func(t *testing.T) {
		bQField := BQField{
			Type: "nullable",
		}
		expectedFieldMode := fieldMode{}

		fieldModeResult, err := bqFieldModeTo(bQField)
		assert.Equal(t, expectedFieldMode, fieldModeResult)
		assert.Nil(t, err)

		bQFieldResult := bqFieldModeFrom(fieldModeResult)
		assert.Equal(t, bQField.Type, bQFieldResult)
	})
	t.Run("should convert from and to BQ Schema successfully", func(t *testing.T) {
		bQSchemaResult, err := bqSchemaTo(schema)
		assert.Equal(t, bQSchema, bQSchemaResult)
		assert.Nil(t, err)

		schemaResult, err := bqSchemaFrom(bQSchemaResult)
		assert.Equal(t, schema, schemaResult)
		assert.Nil(t, err)
	})
	t.Run("bqCreateTableMetaAdapter", func(t *testing.T) {
		t.Run("should convert to BQ TableMetadata", func(t *testing.T) {
			testingProject := "project"
			testingDataset := "dataset"
			testingTable := "table"
			partitionType := "DAY"
			partitionExpiration := int64(720)
			expirationTimeStr := "2022-01-01T00:00:00.00Z"
			expirationTime, _ := time.Parse(time.RFC3339, expirationTimeStr)
			bQTableMetadata := BQTableMetadata{
				Schema: schema,
				Cluster: &BQClusteringInfo{
					Using: []string{sampleFieldName},
				},
				Partition: &BQPartitionInfo{
					Field:      sampleFieldName,
					Type:       partitionType,
					Expiration: partitionExpiration,
				},
				ExpirationTime: expirationTimeStr,
			}
			bQResource := BQTable{
				Project:  testingProject,
				Dataset:  testingDataset,
				Table:    testingTable,
				Metadata: bQTableMetadata,
			}
			expectedTableMetadata := &bigquery.TableMetadata{
				Name:   testingTable,
				Schema: bQSchema,
				TimePartitioning: &bigquery.TimePartitioning{
					Type:       bigquery.DayPartitioningType,
					Expiration: time.Duration(partitionExpiration) * time.Hour,
					Field:      sampleFieldName,
				},
				Clustering: &bigquery.Clustering{
					Fields: []string{sampleFieldName},
				},
				ExpirationTime: expirationTime,
			}

			actualTableMetadata, err := bqCreateTableMetaAdapter(bQResource)
			assert.Equal(t, expectedTableMetadata, actualTableMetadata)
			assert.Nil(t, err)
		})
		t.Run("should return error when parsing expiration time failed", func(t *testing.T) {
			testingProject := "project"
			testingDataset := "dataset"
			testingTable := "table"
			partitionType := "DAY"
			partitionExpiration := int64(720)
			expirationTimeStr := "2022-01-01"
			bQTableMetadata := BQTableMetadata{
				Schema: schema,
				Cluster: &BQClusteringInfo{
					Using: []string{sampleFieldName},
				},
				Partition: &BQPartitionInfo{
					Field:      sampleFieldName,
					Type:       partitionType,
					Expiration: partitionExpiration,
				},
				ExpirationTime: expirationTimeStr,
			}
			bQResource := BQTable{
				Project:  testingProject,
				Dataset:  testingDataset,
				Table:    testingTable,
				Metadata: bQTableMetadata,
			}

			actualTableMetadata, err := bqCreateTableMetaAdapter(bQResource)
			assert.Nil(t, actualTableMetadata)
			assert.NotNil(t, err)
		})
		t.Run("should return error when converting schema failed", func(t *testing.T) {
			testingProject := "project"
			testingDataset := "dataset"
			testingTable := "table"
			partitionType := "DAY"
			partitionExpiration := int64(720)
			expirationTimeStr := "2022-01-01T00:00:00.00Z"
			schema := BQSchema{
				BQField{
					Name:        sampleFieldName,
					Type:        sampleFieldType,
					Description: sampleFieldDescription,
					Mode:        "invalidMode",
					Schema:      BQSchema{},
				},
			}
			bQTableMetadata := BQTableMetadata{
				Schema: schema,
				Cluster: &BQClusteringInfo{
					Using: []string{sampleFieldName},
				},
				Partition: &BQPartitionInfo{
					Field:      sampleFieldName,
					Type:       partitionType,
					Expiration: partitionExpiration,
				},
				ExpirationTime: expirationTimeStr,
			}
			bQResource := BQTable{
				Project:  testingProject,
				Dataset:  testingDataset,
				Table:    testingTable,
				Metadata: bQTableMetadata,
			}

			actualTableMetadata, err := bqCreateTableMetaAdapter(bQResource)
			assert.Nil(t, actualTableMetadata)
			assert.NotNil(t, err)
		})
	})
	t.Run("bqUpdateTableMetaAdapter", func(t *testing.T) {
		t.Run("should convert to BQ TableMetadata", func(t *testing.T) {
			testingProject := "project"
			testingDataset := "dataset"
			testingTable := "table"
			partitionType := "DAY"
			partitionExpiration := int64(720)
			expirationTimeStr := "2022-01-01T00:00:00.00Z"
			expirationTime, _ := time.Parse(time.RFC3339, expirationTimeStr)
			bQTableMetadata := BQTableMetadata{
				Schema: schema,
				Cluster: &BQClusteringInfo{
					Using: []string{sampleFieldName},
				},
				Partition: &BQPartitionInfo{
					Field:      sampleFieldName,
					Type:       partitionType,
					Expiration: partitionExpiration,
				},
				ExpirationTime: expirationTimeStr,
			}
			bQResource := BQTable{
				Project:  testingProject,
				Dataset:  testingDataset,
				Table:    testingTable,
				Metadata: bQTableMetadata,
			}
			expectedTableMetadata := bigquery.TableMetadataToUpdate{
				Name:   testingTable,
				Schema: bQSchema,
				TimePartitioning: &bigquery.TimePartitioning{
					Type:       bigquery.DayPartitioningType,
					Expiration: time.Duration(partitionExpiration) * time.Hour,
					Field:      sampleFieldName,
				},
				ExpirationTime: expirationTime,
			}

			actualTableMetadata, err := bqUpdateTableMetaAdapter(bQResource)
			assert.Equal(t, expectedTableMetadata, actualTableMetadata)
			assert.Nil(t, err)
		})
		t.Run("should return error when table name is empty", func(t *testing.T) {
			testingProject := "project"
			testingDataset := "dataset"
			partitionType := "DAY"
			partitionExpiration := int64(720)
			expirationTimeStr := "2022-01-01"
			bQTableMetadata := BQTableMetadata{
				Schema: schema,
				Cluster: &BQClusteringInfo{
					Using: []string{sampleFieldName},
				},
				Partition: &BQPartitionInfo{
					Field:      sampleFieldName,
					Type:       partitionType,
					Expiration: partitionExpiration,
				},
				ExpirationTime: expirationTimeStr,
			}
			bQResource := BQTable{
				Project:  testingProject,
				Dataset:  testingDataset,
				Metadata: bQTableMetadata,
			}
			expectedTableMetadata := bigquery.TableMetadataToUpdate{}

			actualTableMetadata, err := bqUpdateTableMetaAdapter(bQResource)
			assert.Equal(t, expectedTableMetadata, actualTableMetadata)
			assert.NotNil(t, err)
		})
		t.Run("should return error when parsing expiration time failed", func(t *testing.T) {
			testingProject := "project"
			testingDataset := "dataset"
			testingTable := "table"
			partitionType := "DAY"
			partitionExpiration := int64(720)
			expirationTimeStr := "2022-01-01"
			bQTableMetadata := BQTableMetadata{
				Schema: schema,
				Cluster: &BQClusteringInfo{
					Using: []string{sampleFieldName},
				},
				Partition: &BQPartitionInfo{
					Field:      sampleFieldName,
					Type:       partitionType,
					Expiration: partitionExpiration,
				},
				ExpirationTime: expirationTimeStr,
			}
			bQResource := BQTable{
				Project:  testingProject,
				Dataset:  testingDataset,
				Table:    testingTable,
				Metadata: bQTableMetadata,
			}
			expectedTableMetadata := bigquery.TableMetadataToUpdate{
				Name:   testingTable,
				Schema: bQSchema,
				TimePartitioning: &bigquery.TimePartitioning{
					Type:       bigquery.DayPartitioningType,
					Expiration: time.Duration(partitionExpiration) * time.Hour,
					Field:      sampleFieldName,
				},
			}

			actualTableMetadata, err := bqUpdateTableMetaAdapter(bQResource)
			assert.Equal(t, expectedTableMetadata, actualTableMetadata)
			assert.NotNil(t, err)
		})
		t.Run("should return error when converting schema failed", func(t *testing.T) {
			testingProject := "project"
			testingDataset := "dataset"
			testingTable := "table"
			partitionType := "DAY"
			partitionExpiration := int64(720)
			expirationTimeStr := "2022-01-01T00:00:00.00Z"
			schema := BQSchema{
				BQField{
					Name:        sampleFieldName,
					Type:        sampleFieldType,
					Description: sampleFieldDescription,
					Mode:        "invalidMode",
					Schema:      BQSchema{},
				},
			}
			bQTableMetadata := BQTableMetadata{
				Schema: schema,
				Cluster: &BQClusteringInfo{
					Using: []string{sampleFieldName},
				},
				Partition: &BQPartitionInfo{
					Field:      sampleFieldName,
					Type:       partitionType,
					Expiration: partitionExpiration,
				},
				ExpirationTime: expirationTimeStr,
			}
			bQResource := BQTable{
				Project:  testingProject,
				Dataset:  testingDataset,
				Table:    testingTable,
				Metadata: bQTableMetadata,
			}
			expectedTableMetadata := bigquery.TableMetadataToUpdate{
				Name: testingTable,
				TimePartitioning: &bigquery.TimePartitioning{
					Type:       bigquery.DayPartitioningType,
					Expiration: time.Duration(partitionExpiration) * time.Hour,
					Field:      sampleFieldName,
				},
			}

			actualTableMetadata, err := bqUpdateTableMetaAdapter(bQResource)
			assert.Equal(t, expectedTableMetadata, actualTableMetadata)
			assert.NotNil(t, err)
		})
	})
}
