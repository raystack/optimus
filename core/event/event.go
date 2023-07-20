package event

import (
	"time"

	"github.com/google/uuid"

	"github.com/raystack/optimus/internal/errors"
)

const eventsEntity = "events"

type Event struct {
	ID         uuid.UUID
	OccurredAt time.Time
}

func NewBaseEvent() (Event, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return Event{}, errors.InternalError(eventsEntity, "not able to generate event uuid", err)
	}
	return Event{
		ID:         id,
		OccurredAt: time.Now(),
	}, nil
}
