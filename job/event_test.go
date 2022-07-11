package job_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestEventService(t *testing.T) {
	l := log.NewNoop()
	eventValues, _ := structpb.NewStruct(
		map[string]interface{}{
			"url": "https://example.io",
		},
	)
	t.Run("should successfully notify registered notifiers on valid event", func(t *testing.T) {
		projectSpec := models.ProjectSpec{
			ID:   models.ProjectID(uuid.New()),
			Name: "a-data-project",
		}

		namespaceSpec := models.NamespaceSpec{
			ID:          uuid.New(),
			Name:        "game_jam",
			ProjectSpec: projectSpec,
		}
		jobSpec := models.JobSpec{
			Name: "transform-tables",
			Behavior: models.JobSpecBehavior{
				Notify: []models.JobSpecNotifier{
					{
						On: models.JobFailureEvent,
						Channels: []string{
							"slacker://@devs",
						},
					},
				},
			},
		}
		je := models.JobEvent{
			Type:  models.JobFailureEvent,
			Value: eventValues.Fields,
		}

		notifier := new(mock.Notifier)
		notifier.On("Notify", context.Background(), models.NotifyAttrs{
			Namespace: namespaceSpec,
			JobSpec:   jobSpec,
			JobEvent:  je,
			Route:     "@devs",
		}).Return(nil)
		defer notifier.AssertExpectations(t)

		evtService := job.NewEventService(l, map[string]models.Notifier{
			"slacker": notifier,
		})
		err := evtService.Register(context.Background(), namespaceSpec, jobSpec, je)
		assert.Nil(t, err)
	})
	t.Run("should ignore notify events for unknown schemes", func(t *testing.T) {
		projectSpec := models.ProjectSpec{
			ID:   models.ProjectID(uuid.New()),
			Name: "a-data-project",
		}

		namespaceSpec := models.NamespaceSpec{
			ID:          uuid.New(),
			Name:        "game_jam",
			ProjectSpec: projectSpec,
		}
		jobSpec := models.JobSpec{
			Name: "transform-tables",
			Behavior: models.JobSpecBehavior{
				Notify: []models.JobSpecNotifier{
					{
						On: models.JobFailureEvent,
						Channels: []string{
							"blocker://@devs",
						},
					},
				},
			},
		}
		je := models.JobEvent{
			Type:  models.JobFailureEvent,
			Value: eventValues.GetFields(),
		}

		notifier := new(mock.Notifier)
		defer notifier.AssertExpectations(t)

		evtService := job.NewEventService(l, map[string]models.Notifier{
			"slacker": notifier,
		})
		err := evtService.Register(context.Background(), namespaceSpec, jobSpec, je)
		assert.Nil(t, err)
	})
	t.Run("should fail if failed to notify registered notifiers on valid event", func(t *testing.T) {
		projectSpec := models.ProjectSpec{
			ID:   models.ProjectID(uuid.New()),
			Name: "a-data-project",
		}

		namespaceSpec := models.NamespaceSpec{
			ID:          uuid.New(),
			Name:        "game_jam",
			ProjectSpec: projectSpec,
		}
		jobSpec := models.JobSpec{
			Name: "transform-tables",
			Behavior: models.JobSpecBehavior{
				Notify: []models.JobSpecNotifier{
					{
						On: models.JobFailureEvent,
						Channels: []string{
							"slacker://@devs",
						},
					},
				},
			},
		}
		je := models.JobEvent{
			Type:  models.JobFailureEvent,
			Value: eventValues.GetFields(),
		}

		notifier := new(mock.Notifier)
		notifier.On("Notify", context.Background(), models.NotifyAttrs{
			Namespace: namespaceSpec,
			JobSpec:   jobSpec,
			JobEvent:  je,
			Route:     "@devs",
		}).Return(errors.New("failed to notify"))
		defer notifier.AssertExpectations(t)

		evtService := job.NewEventService(l, map[string]models.Notifier{
			"slacker": notifier,
		})
		err := evtService.Register(context.Background(), namespaceSpec, jobSpec, je)
		assert.Error(t, err, "failed to notify")
	})
}
