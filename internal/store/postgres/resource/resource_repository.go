package resource

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

type Resource struct {
	ID uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

	ProjectName   string `json:"project_name"`
	NamespaceName string `json:"namespace_name"`

	Version   int
	Name      string `gorm:"not null"`
	Type      string `gorm:"not null"`
	Datastore string `gorm:"not null"`
	URN       string `gorm:"not null"`

	Spec   []byte
	Labels datatypes.JSON

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}
