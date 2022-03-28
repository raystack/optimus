package utils

import "github.com/google/uuid"

type UUIDProvider interface {
	NewUUID() (uuid.UUID, error)
}

type uuidProvider struct{}

func (*uuidProvider) NewUUID() (uuid.UUID, error) {
	return uuid.NewRandom()
}

func NewUUIDProvider() *uuidProvider {
	return &uuidProvider{}
}
