package postgres

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type Replay struct {
	ID uuid.UUID `gorm:"primary_key;type:uuid"`

	JobID uuid.UUID `gorm:"not null"`
	Job   Job       `gorm:"foreignKey:JobID"`

	ProjectID uuid.UUID `gorm:"not null"`
	Project   Project   `gorm:"foreignKey:ProjectID"`

	StartDate time.Time
	EndDate   time.Time
	Status    string
	Message   string
	CommitID  string

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

func (p Replay) FromSpec(spec *models.ReplaySpec) (Replay, error) {
	return Replay{
		ID:        spec.ID,
		JobID:     spec.Job.ID,
		ProjectID: spec.Project.ID,
		StartDate: spec.StartDate,
		EndDate:   spec.EndDate,
		Status:    spec.Status,
		CommitID:  spec.CommitID,
		Message:   spec.Message,
	}, nil
}

func (p Replay) ToSpec() (models.ReplaySpec, error) {
	return models.ReplaySpec{
		ID:        p.ID,
		Status:    p.Status,
		StartDate: p.StartDate,
		EndDate:   p.EndDate,
		Message:   p.Message,
		CommitID:  p.CommitID,
	}, nil
}

type replayRepository struct {
	DB *gorm.DB
}

func NewReplayRepository(db *gorm.DB) *replayRepository {
	return &replayRepository{
		DB: db,
	}
}

func (repo *replayRepository) Insert(replay *models.ReplaySpec) error {
	r, err := Replay{}.FromSpec(replay)
	if err != nil {
		return err
	}
	return repo.DB.Create(&r).Error
}

func (repo *replayRepository) GetByID(id uuid.UUID) (models.ReplaySpec, error) {
	var r Replay
	if err := repo.DB.Where("id = ?", id).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ReplaySpec{}, store.ErrResourceNotFound
		}
		return models.ReplaySpec{}, err
	}
	return r.ToSpec()
}
