ALTER TABLE IF EXISTS secret
    RENAME TO secret_old;

CREATE TABLE IF NOT EXISTS secret (
  name VARCHAR(100) NOT NULL,
  id UUID NOT NULL UNIQUE DEFAULT uuid_generate_v4(),
  value TEXT NOT NULL,
  type VARCHAR(15) NOT NULL,

  project_name VARCHAR(100) NOT NULL,
  namespace_name VARCHAR(100),

  created_at TIMESTAMP WITH TIME ZONE NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
  deleted_at TIMESTAMP WITH TIME ZONE,

  PRIMARY KEY (project_name, name),
  FOREIGN KEY (project_name) REFERENCES project (name),
  FOREIGN KEY (project_name, namespace_name) REFERENCES namespace (project_name, name)
);

INSERT INTO secret (name, id, value, type, project_name, namespace_name, created_at, updated_at, deleted_at)
SELECT s.name, s.id, s.value, s.type, p.name, n.name, s.created_at, s.updated_at, s.deleted_at
FROM secret_old s
    LEFT JOIN namespace_old n ON n.id = s.id
    JOIN project_old p on p.id = s.project_id
WHERE n.deleted_at IS NULL;
