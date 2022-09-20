package postgres_test

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

func readStoredRecords[R any](db *gorm.DB) []R {
	return readStoredRecordsByFilter[R](db, nil)
}

func readStoredRecordsByFilter[R any](db *gorm.DB, columnToValue map[string]interface{}) []R {
	var filters []string
	var values []interface{}
	for column, value := range columnToValue {
		filters = append(filters, fmt.Sprintf("%s = ?", column))
		values = append(values, value)
	}

	find := db
	if len(filters) > 0 {
		f := strings.Join(filters, " and ")
		find = find.Where(f, values...)
	}

	var output []R
	if err := find.Find(&output).Error; err != nil {
		panic(err)
	}
	return output
}

func insertRecords[R any](db *gorm.DB, records []R) {
	err := db.Transaction(func(tx *gorm.DB) error {
		for _, r := range records {
			if err := tx.Create(r).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}
