package moderator_test

import (
	"errors"
	"testing"
	"time"

	"github.com/raystack/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/raystack/optimus/core/event/moderator"
)

func TestHandleEvent(t *testing.T) {
	const buffer = 8
	logger := log.NewNoop()
	timeout := 1 * time.Second

	t.Run("do not send message if there is error in extracting bytes of the event", func(t *testing.T) {
		messageChan := make(chan []byte, buffer)
		handler := moderator.NewEventHandler(messageChan, logger)

		event := NewEvent(t)
		event.On("Bytes").Return(nil, errors.New("cannot get bytes representation"))

		handler.HandleEvent(event)

		timer := time.NewTimer(timeout)
		var receivedEventBytes []byte
		for {
			done := false
			select {
			case bytes := <-messageChan:
				receivedEventBytes = bytes
			case <-timer.C:
				done = true
			}
			if done {
				break
			}
		}
		assert.Nil(t, receivedEventBytes)
	})

	t.Run("send message if no error is found when extracting bytes from the event", func(t *testing.T) {
		messageChan := make(chan []byte, buffer)
		handler := moderator.NewEventHandler(messageChan, logger)

		sentEventBytes := []byte("message")
		event := NewEvent(t)
		event.On("Bytes").Return(sentEventBytes, nil)

		handler.HandleEvent(event)

		timer := time.NewTimer(timeout)
		var receivedEventBytes []byte
		for {
			done := false
			select {
			case bytes := <-messageChan:
				receivedEventBytes = bytes
			case <-timer.C:
				done = true
			}
			if done {
				break
			}
		}
		assert.EqualValues(t, sentEventBytes, receivedEventBytes)
	})
}

type Event struct {
	mock.Mock
}

// Bytes provides a mock function with given fields:
func (_m *Event) Bytes() ([]byte, error) {
	ret := _m.Called()

	var r0 []byte
	var r1 error
	if rf, ok := ret.Get(0).(func() ([]byte, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() []byte); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewEvent interface {
	mock.TestingT
	Cleanup(func())
}

// NewEvent creates a new instance of Event. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewEvent(t mockConstructorTestingTNewEvent) *Event {
	mock := &Event{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
