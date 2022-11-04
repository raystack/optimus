package job

type Metadata struct {
	resource  *ResourceMetadata
	scheduler map[string]string
}

func (m Metadata) Resource() *ResourceMetadata {
	return m.resource
}

func (m Metadata) Scheduler() map[string]string {
	return m.scheduler
}

func NewMetadata(resource *ResourceMetadata, scheduler map[string]string) *Metadata {
	return &Metadata{resource: resource, scheduler: scheduler}
}

type ResourceMetadata struct {
	request *ResourceConfig
	limit   *ResourceConfig
}

func (r ResourceMetadata) Request() *ResourceConfig {
	return r.request
}

func (r ResourceMetadata) Limit() *ResourceConfig {
	return r.limit
}

func NewResourceMetadata(request *ResourceConfig, limit *ResourceConfig) *ResourceMetadata {
	return &ResourceMetadata{request: request, limit: limit}
}

type ResourceConfig struct {
	cpu    string
	memory string
}

func (r ResourceConfig) CPU() string {
	return r.cpu
}

func (r ResourceConfig) Memory() string {
	return r.memory
}

func NewResourceConfig(cpu string, memory string) *ResourceConfig {
	return &ResourceConfig{cpu: cpu, memory: memory}
}
