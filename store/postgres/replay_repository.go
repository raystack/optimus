package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type Replay struct {
	ID uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

	JobID uuid.UUID `gorm:"not null"`
	Job   Job       `gorm:"foreignKey:JobID"`

	StartDate     time.Time `gorm:"not null"`
	EndDate       time.Time `gorm:"not null"`
	Status        string    `gorm:"not null"`
	Message       datatypes.JSON
	ExecutionTree datatypes.JSON
	Config        datatypes.JSON

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

type ExecutionTree struct {
	JobSpec    Job
	Dependents []*ExecutionTree
	Runs       []time.Time
}

func fromTreeNode(treeNode *tree.TreeNode) *ExecutionTree {
	// only store necessary job spec data in tree
	treeNodeJob := treeNode.Data.(models.JobSpec)
	jobSpec := Job{
		ID:          treeNodeJob.ID,
		Version:     treeNodeJob.Version,
		Name:        treeNodeJob.Name,
		Owner:       treeNodeJob.Owner,
		Description: treeNodeJob.Description,
		StartDate:   treeNodeJob.Schedule.StartDate,
		EndDate:     treeNodeJob.Schedule.EndDate,
		Interval:    &treeNodeJob.Schedule.Interval,
	}

	var dependents []*ExecutionTree
	for _, dependent := range treeNode.Dependents {
		dependents = append(dependents, fromTreeNode(dependent))
	}

	var runs []time.Time
	for _, run := range treeNode.Runs.Values() {
		runs = append(runs, run.(time.Time))
	}

	return &ExecutionTree{
		JobSpec:    jobSpec,
		Dependents: dependents,
		Runs:       runs,
	}
}

func (p Replay) FromSpec(spec *models.ReplaySpec) (Replay, error) {
	message, err := json.Marshal(spec.Message)
	if err != nil {
		return Replay{}, nil
	}

	var executionTree []byte
	if spec.ExecutionTree != nil {
		executionTree, err = json.Marshal(fromTreeNode(spec.ExecutionTree))
		if err != nil {
			return Replay{}, err
		}
	}

	configInBytes, err := json.Marshal(spec.Config)
	if err != nil {
		return Replay{}, err
	}

	return Replay{
		ID:            spec.ID,
		JobID:         spec.Job.ID,
		StartDate:     spec.StartDate.UTC(),
		EndDate:       spec.EndDate.UTC(),
		Status:        spec.Status,
		Config:        configInBytes,
		Message:       message,
		ExecutionTree: executionTree,
	}, nil
}

func toTreeNode(executionTree *ExecutionTree) *tree.TreeNode {
	jobSpec := models.JobSpec{
		ID:          executionTree.JobSpec.ID,
		Version:     executionTree.JobSpec.Version,
		Name:        executionTree.JobSpec.Name,
		Owner:       executionTree.JobSpec.Owner,
		Description: executionTree.JobSpec.Description,
		Schedule: models.JobSpecSchedule{
			StartDate: executionTree.JobSpec.StartDate,
			EndDate:   executionTree.JobSpec.EndDate,
			Interval:  *executionTree.JobSpec.Interval,
		},
	}
	treeNode := tree.NewTreeNode(jobSpec)
	for _, dependent := range executionTree.Dependents {
		treeNode.AddDependent(toTreeNode(dependent))
	}
	for _, run := range executionTree.Runs {
		treeNode.Runs.Add(run)
	}
	return treeNode
}

func (p Replay) ToSpec(jobSpec models.JobSpec) (models.ReplaySpec, error) {
	message := models.ReplayMessage{}
	if err := json.Unmarshal(p.Message, &message); err != nil {
		return models.ReplaySpec{}, nil
	}

	var treeNode *tree.TreeNode
	if p.ExecutionTree != nil {
		jobTree := ExecutionTree{}
		if err := json.Unmarshal(p.ExecutionTree, &jobTree); err != nil {
			return models.ReplaySpec{}, err
		}
		treeNode = toTreeNode(&jobTree)
	}

	if p.Config != nil {
		config := make(map[string]string)
		if err := json.Unmarshal(p.Config, &config); err != nil {
			return models.ReplaySpec{}, err
		}
	}

	return models.ReplaySpec{
		ID:            p.ID,
		Job:           jobSpec,
		Status:        p.Status,
		StartDate:     p.StartDate,
		EndDate:       p.EndDate,
		Message:       message,
		ExecutionTree: treeNode,
		CreatedAt:     p.CreatedAt,
	}, nil
}

type replayRepository struct {
	DB      *gorm.DB
	adapter *JobSpecAdapter
}

func NewReplayRepository(db *gorm.DB, jobAdapter *JobSpecAdapter) *replayRepository {
	return &replayRepository{
		DB:      db,
		adapter: jobAdapter,
	}
}

func (repo *replayRepository) Insert(ctx context.Context, replay *models.ReplaySpec) error {
	r, err := Replay{}.FromSpec(replay)
	if err != nil {
		return err
	}
	return repo.DB.WithContext(ctx).Create(&r).Error
}

func (repo *replayRepository) GetByID(ctx context.Context, id uuid.UUID) (models.ReplaySpec, error) {
	var r Replay
	if err := repo.DB.WithContext(ctx).Where("id = ?", id).Preload("Job").First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ReplaySpec{}, store.ErrResourceNotFound
		}
		return models.ReplaySpec{}, err
	}
	jobSpec, err := repo.adapter.ToSpec(r.Job)
	if err != nil {
		return models.ReplaySpec{}, err
	}
	return r.ToSpec(jobSpec)
}

func (repo *replayRepository) UpdateStatus(ctx context.Context, replayID uuid.UUID, status string, message models.ReplayMessage) error {
	var r Replay
	if err := repo.DB.WithContext(ctx).Where("id = ?", replayID).Find(&r).Error; err != nil {
		return errors.New("could not update non-existing replay")
	}
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	r.Status = status
	r.Message = jsonBytes
	return repo.DB.WithContext(ctx).Save(&r).Error
}

func (repo *replayRepository) GetByStatus(ctx context.Context, status []string) ([]models.ReplaySpec, error) {
	var replays []Replay
	if err := repo.DB.WithContext(ctx).Where("status in (?)", status).Preload("Job").Find(&replays).Error; err != nil {
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

func (repo *replayRepository) GetByJobIDAndStatus(ctx context.Context, jobID uuid.UUID, status []string) ([]models.ReplaySpec, error) {
	var replays []Replay
	if err := repo.DB.WithContext(ctx).Where("job_id = ? and status in (?)", jobID, status).Preload("Job").Find(&replays).Error; err != nil {
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

func (repo *replayRepository) GetByProjectIDAndStatus(ctx context.Context, projectID models.ProjectID, status []string) ([]models.ReplaySpec, error) {
	var replays []Replay
	if err := repo.DB.WithContext(ctx).Preload("Job").Joins("JOIN job ON replay.job_id = job.id").
		Where("job.project_id = ? and status in (?)", projectID.UUID(), status).Find(&replays).Error; err != nil {
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

func (repo *replayRepository) GetByProjectID(ctx context.Context, projectID models.ProjectID) ([]models.ReplaySpec, error) {
	var replays []Replay
	if err := repo.DB.WithContext(ctx).Preload("Job").Joins("JOIN job ON replay.job_id = job.id").
		Where("job.project_id = ?", projectID.UUID()).Order("created_at DESC").Find(&replays).Error; err != nil {
		return []models.ReplaySpec{}, err
	}

	if len(replays) == 0 {
		return []models.ReplaySpec{}, store.ErrResourceNotFound
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
