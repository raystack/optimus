ALTER TABLE secret DROP IF EXISTS namespace_id;
ALTER TABLE secret DROP IF EXISTS type;

DROP INDEX IF EXISTS secret_namespace_id_idx;
DROP INDEX IF EXISTS secret_type_idx;