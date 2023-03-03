package resource

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

const (
	columnsToStore  = `full_name, kind, store, status, urn, project_name, namespace_name, metadata, spec, created_at, updated_at`
	resourceColumns = `id, ` + columnsToStore
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{
		db: pool,
	}
}

func (r Repository) Create(ctx context.Context, resourceModel *resource.Resource) error {
	res := FromResourceToModel(resourceModel)

	insertResource := `INSERT INTO resource (` + columnsToStore + `) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), now())`
	_, err := r.db.Exec(ctx, insertResource, res.FullName, res.Kind, res.Store, res.Status, res.URN,
		res.ProjectName, res.NamespaceName, res.Metadata, res.Spec)
	return errors.WrapIfErr(tenant.EntityNamespace, "error creating resource to database", err)
}

func (r Repository) Update(ctx context.Context, resourceModel *resource.Resource) error {
	res := FromResourceToModel(resourceModel)

	updateResource := `UPDATE resource SET kind=$1, status=$2, urn=$3, metadata=$4, spec=$5, updated_at=now() 
                WHERE full_name=$6 AND store=$7 AND project_name = $8 And namespace_name = $9`
	tag, err := r.db.Exec(ctx, updateResource, res.Kind, res.Status, res.URN,
		res.Metadata, res.Spec, res.FullName, res.Store, res.ProjectName, res.NamespaceName)
	if err != nil {
		return errors.Wrap(resource.EntityResource, "error updating resource to database", err)
	}

	if tag.RowsAffected() == 0 {
		return errors.NotFound(resource.EntityResource, "no resource to update for "+res.FullName)
	}
	return nil
}

func (r Repository) ReadByFullName(ctx context.Context, tnnt tenant.Tenant, store resource.Store, fullName string) (*resource.Resource, error) {
	var res Resource
	getResource := `SELECT ` + resourceColumns + ` FROM resource WHERE full_name = $1 AND store = $2 AND
	project_name = $3 AND namespace_name = $4`
	err := r.db.QueryRow(ctx, getResource, fullName, store, tnnt.ProjectName(), tnnt.NamespaceName()).
		Scan(&res.ID, &res.FullName, &res.Kind, &res.Store, &res.Status, &res.URN,
			&res.ProjectName, &res.NamespaceName, &res.Metadata, &res.Spec, &res.CreatedAt, &res.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(resource.EntityResource, "no resource found for "+res.FullName)
		}

		return nil, errors.Wrap(resource.EntityResource, "error reading the resource "+res.FullName, err)
	}

	return FromModelToResource(&res)
}

func (r Repository) ReadAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error) {
	getAllResources := `SELECT ` + resourceColumns + ` FROM resource WHERE project_name = $1 and namespace_name = $2 and store = $3`
	rows, err := r.db.Query(ctx, getAllResources, tnnt.ProjectName(), tnnt.NamespaceName(), store)
	if err != nil {
		return nil, errors.Wrap(resource.EntityResource, "error in ReadAll", err)
	}
	defer rows.Close()

	var resources []*resource.Resource
	for rows.Next() {
		var res Resource
		err = rows.Scan(&res.ID, &res.FullName, &res.Kind, &res.Store, &res.Status, &res.URN,
			&res.ProjectName, &res.NamespaceName, &res.Metadata, &res.Spec, &res.CreatedAt, &res.UpdatedAt)
		if err != nil {
			return nil, errors.Wrap(resource.EntityResource, "error in GetAll", err)
		}

		resourceModel, err := FromModelToResource(&res)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resourceModel)
	}

	return resources, nil
}

func (r Repository) GetResources(ctx context.Context, tnnt tenant.Tenant, store resource.Store, names []string) ([]*resource.Resource, error) {
	getAllResources := `SELECT ` + resourceColumns + ` FROM resource WHERE project_name = $1 and namespace_name = $2 and 
store = $3 AND full_name = any ($4)`
	rows, err := r.db.Query(ctx, getAllResources, tnnt.ProjectName(), tnnt.NamespaceName(), store, names)
	if err != nil {
		return nil, errors.Wrap(resource.EntityResource, "error in ReadAll", err)
	}
	defer rows.Close()

	var resources []*resource.Resource
	for rows.Next() {
		var res Resource
		err = rows.Scan(&res.ID, &res.FullName, &res.Kind, &res.Store, &res.Status, &res.URN,
			&res.ProjectName, &res.NamespaceName, &res.Metadata, &res.Spec, &res.CreatedAt, &res.UpdatedAt)
		if err != nil {
			return nil, errors.Wrap(resource.EntityResource, "error in GetAll", err)
		}

		resourceModel, err := FromModelToResource(&res)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resourceModel)
	}

	return resources, nil
}

func (r Repository) UpdateStatus(ctx context.Context, resources ...*resource.Resource) error {
	batch := pgx.Batch{}
	for _, res := range resources {
		updateStatus := `UPDATE resource SET status = $1 WHERE project_name = $2 AND namespace_name = $3 AND store = $4 AND full_name = $5`
		batch.Queue(updateStatus, res.Status(), res.Tenant().ProjectName(), res.Tenant().NamespaceName(), res.Store(), res.FullName())
	}

	results := r.db.SendBatch(ctx, &batch)
	defer results.Close()

	multiErr := errors.NewMultiError("error updating resources status")
	for i := range resources {
		tag, err := results.Exec()
		multiErr.Append(err)
		if tag.RowsAffected() == 0 {
			multiErr.Append(errors.InternalError(resource.EntityResource, "error updating status for "+resources[i].FullName(), nil))
		}
	}

	return errors.MultiToError(multiErr)
}
