package job_run

type ConfigMap map[string]string

type RunInput struct {
	Configs ConfigMap
	Secrets ConfigMap
	Files   ConfigMap
}
