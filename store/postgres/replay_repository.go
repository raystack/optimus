package postgres

import (
	"encoding/json"
	"errors"
	"time"

	"gorm.io/datatypes"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type Replay struct {
	ID uuid.UUID `gorm:"primary_key;type:uuid"`

	JobID uuid.UUID `gorm:"not null"`
	Job   Job       `gorm:"foreignKey:JobID"`

	StartDate time.Time `gorm:"not null"`
	EndDate   time.Time `gorm:"not null"`
	Status    string    `gorm:"not null"`
	Message   datatypes.JSON

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

func (p Replay) FromSpec(spec *models.ReplaySpec) (Replay, error) {
	jsonBytes, err := json.Marshal(spec.Message)
	if err != nil {
		return Replay{}, nil
	}
	return Replay{
		ID:        spec.ID,
		JobID:     spec.Job.ID,
		StartDate: spec.StartDate,
		EndDate:   spec.EndDate,
		Status:    spec.Status,
		Message:   jsonBytes,
	}, nil
}

func (p Replay) ToSpec(jobSpec models.JobSpec) (models.ReplaySpec, error) {
	message := models.ReplayMessage{}
	if err := json.Unmarshal(p.Message, &message); err != nil {
		return models.ReplaySpec{}, nil
	}
	return models.ReplaySpec{
		ID:        p.ID,
		Job:       jobSpec,
		Status:    p.Status,
		StartDate: p.StartDate,
		EndDate:   p.EndDate,
		Message:   message,
	}, nil
}

type replayRepository struct {
	DB      *gorm.DB
	jobSpec models.JobSpec
	adapter *JobSpecAdapter
}

func NewReplayRepository(db *gorm.DB, jobSpec models.JobSpec, jobAdapter *JobSpecAdapter) *replayRepository {
	return &replayRepository{
		DB:      db,
		jobSpec: jobSpec,
		adapter: jobAdapter,
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
	return r.ToSpec(repo.jobSpec)
}

func (repo *replayRepository) UpdateStatus(replayID uuid.UUID, status string, message models.ReplayMessage) error {
	var r Replay
	if err := repo.DB.Where("id = ?", replayID).Find(&r).Error; err != nil {
		return errors.New("could not update non-existing replay")
	}
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	r.Status = status
	r.Message = jsonBytes
	return repo.DB.Save(&r).Error
}

func (repo *replayRepository) GetByStatus(status []string) ([]models.ReplaySpec, error) {
	var replays []Replay
	if err := repo.DB.Where("status in (?)", status).Preload("Job").Find(&replays).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return []models.ReplaySpec{}, store.ErrResourceNotFound
		}
		return []models.ReplaySpec{}, err
	}

	var replaySpecs []models.ReplaySpec
	for _, r := range replays {
		jobSpec, err := repo.adapter.ToSpec(r.Job)
		if err != nil {
			return []models.ReplaySpec{}, err
		}
		replaySpec, err := r.ToSpec(jobSpec)
		if err != nil {
			return []models.ReplaySpec{}, err
		}
		replaySpecs = append(replaySpecs, replaySpec)
	}
	return replaySpecs, nil
}
