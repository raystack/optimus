// Code generated by mockery v2.14.0. DO NOT EDIT.

package mock

import (
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/internal/writer"
)

// LogWriter is an autogenerated mock type for the LogWriter type
type LogWriter struct {
	mock.Mock
}

// Write provides a mock function with given fields: _a0, _a1
func (_m *LogWriter) Write(_a0 writer.LogLevel, _a1 string) error {
	ret := _m.Called(_a0, _a1)

	var r0 error
	if rf, ok := ret.Get(0).(func(writer.LogLevel, string) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewLogWriter interface {
	mock.TestingT
	Cleanup(func())
}

// NewLogWriter creates a new instance of LogWriter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewLogWriter(t mockConstructorTestingTNewLogWriter) *LogWriter {
	mock := &LogWriter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
