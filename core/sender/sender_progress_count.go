package sender

type ProgressCount interface {
	Add(count int) error
	Inc() error
}
