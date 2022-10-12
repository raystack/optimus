CREATE TABLE job_run_old (
	id uuid NOT NULL DEFAULT uuid_generate_v4(),
	job_id uuid NULL,
	namespace_id uuid NULL,
	specification jsonb NULL,
	scheduled_at timestamptz NOT NULL,
	status varchar(30) NOT NULL,
	"trigger" varchar(30) NOT NULL,
	"data" jsonb NULL,
	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL,
	CONSTRAINT job_run_pkey PRIMARY KEY (id)
);
CREATE INDEX job_run_job_id_idx ON job_run_old USING btree (job_id);
CREATE INDEX job_run_namespace_id_idx ON job_run_old USING btree (namespace_id);
CREATE INDEX job_run_status_idx ON job_run_old USING btree (status);
CREATE INDEX job_run_trigger_idx ON job_run_old USING btree (trigger);