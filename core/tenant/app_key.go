package tenant

import (
	"bytes"
	"io"

	"github.com/odpf/optimus/internal/errors"
)

const (
	keyLength = 32
)

type ApplicationKey struct {
	key *[keyLength]byte
}

func (s *ApplicationKey) GetKey() *[32]byte {
	return s.key
}

func NewApplicationKey(k string) (ApplicationKey, error) {
	key := ApplicationKey{
		key: &[keyLength]byte{},
	}

	if len(k) < keyLength {
		return key, errors.InvalidArgument("application_key", "random hash should be 32 chars in length")
	}

	_, err := io.ReadFull(bytes.NewBufferString(k), key.key[:])
	return key, err
}
