package resource

import (
	"time"

	"gorm.io/datatypes"
)

type Resource struct {
	FullName string `gorm:"not null"`
	Spec     datatypes.JSON
	Metadata datatypes.JSON

	ProjectName   string `json:"project_name"`
	NamespaceName string `json:"namespace_name"`

	Type      string `gorm:"not null"`
	Datastore string `gorm:"not null"`
	URN       string `gorm:"not null"`

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

/*
* create a new `resource`` table
* rename the previous version into `resource_old``
 */
