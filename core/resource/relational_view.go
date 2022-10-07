package resource

type View struct {
	Name    Name
	Dataset Dataset

	Description string
	ViewQuery   string
}
