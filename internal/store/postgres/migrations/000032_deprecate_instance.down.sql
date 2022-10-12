CREATE TABLE "instance" (
	id uuid NOT NULL DEFAULT uuid_generate_v4(),
	job_run_id uuid NOT NULL,
	instance_name varchar(150) NULL,
	instance_type varchar(50) NULL,
	executed_at timestamp NULL,
	status varchar(30) NULL,
	"data" jsonb NULL,
	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL,
	CONSTRAINT instance_pkey PRIMARY KEY (id)
);
CREATE INDEX instance_job_run_id_idx ON instance USING btree (job_run_id);

ALTER TABLE "instance" ADD CONSTRAINT instance_job_run_id_fkey FOREIGN KEY (job_run_id) REFERENCES job_run_old(id) ON DELETE CASCADE;