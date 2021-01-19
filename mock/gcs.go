package mock

import (
	"context"
	"io"

	"github.com/stretchr/testify/mock"
)

type ObjectWriter struct {
	mock.Mock
}

func (m *ObjectWriter) NewWriter(ctx context.Context, bucket, path string) (io.WriteCloser, error) {
	args := m.Called(ctx, bucket, path)
	return args.Get(0).(io.WriteCloser), args.Error(1)
}

// mock write closer
type WriteCloser struct {
	mock.Mock
}

func (wc *WriteCloser) Write(p []byte) (n int, err error) {
	args := wc.Called()
	err = args.Error(1)
	if err != nil {
		return
	}
	return args.Get(0).(io.Writer).Write(p)
}

func (wc *WriteCloser) Close() error {
	return wc.Called().Error(0)
}
