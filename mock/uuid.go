package mock

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
)

type UUIDProvider struct {
	mock.Mock
}

func (up *UUIDProvider) NewUUID() (uuid.UUID, error) {
	args := up.Called()
	return args.Get(0).(uuid.UUID), args.Error(1)
}
