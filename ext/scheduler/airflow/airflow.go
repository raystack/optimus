package airflow

type Bucket interface {
	//WriteAll(ctx context.Context, key string, p []byte, opts *blob.WriterOptions) error
	//ReadAll(ctx context.Context, key string) ([]byte, error)
	//List(opts *blob.ListOptions) *blob.ListIterator
	//Delete(ctx context.Context, key string) error
	//Close() error
}
