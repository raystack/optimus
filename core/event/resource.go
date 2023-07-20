package event

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/raystack/optimus/core/resource"
	"github.com/raystack/optimus/internal/errors"
	pbCore "github.com/raystack/optimus/protos/raystack/optimus/core/v1beta1"
	pbInt "github.com/raystack/optimus/protos/raystack/optimus/integration/v1beta1"
)

type ResourceCreated struct {
	Event

	Resource *resource.Resource
}

func NewResourceCreatedEvent(rsc *resource.Resource) (*ResourceCreated, error) {
	baseEvent, err := NewBaseEvent()
	if err != nil {
		return nil, err
	}
	return &ResourceCreated{
		Event:    baseEvent,
		Resource: rsc,
	}, nil
}

func (r ResourceCreated) Bytes() ([]byte, error) {
	return resourceEventToBytes(r.Event, r.Resource, pbInt.OptimusChangeEvent_EVENT_TYPE_RESOURCE_CREATE)
}

type ResourceUpdated struct {
	Event

	Resource *resource.Resource
}

func NewResourceUpdatedEvent(rsc *resource.Resource) (*ResourceUpdated, error) {
	baseEvent, err := NewBaseEvent()
	if err != nil {
		return nil, err
	}
	return &ResourceUpdated{
		Event:    baseEvent,
		Resource: rsc,
	}, nil
}

func (r ResourceUpdated) Bytes() ([]byte, error) {
	return resourceEventToBytes(r.Event, r.Resource, pbInt.OptimusChangeEvent_EVENT_TYPE_RESOURCE_UPDATE)
}

func resourceEventToBytes(event Event, rsc *resource.Resource, eventType pbInt.OptimusChangeEvent_EventType) ([]byte, error) {
	meta := rsc.Metadata()
	if meta == nil {
		return nil, errors.InvalidArgument(resource.EntityResource, "missing resource metadata")
	}

	pbStruct, err := structpb.NewStruct(rsc.Spec())
	if err != nil {
		return nil, errors.InvalidArgument(resource.EntityResource, "unable to convert spec to proto struct")
	}

	resourcePb := &pbCore.ResourceSpecification{
		Version: meta.Version,
		Name:    rsc.FullName(),
		Type:    rsc.Kind(),
		Spec:    pbStruct,
		Assets:  nil,
		Labels:  meta.Labels,
	}
	occurredAt := timestamppb.New(event.OccurredAt)
	optEvent := &pbInt.OptimusChangeEvent{
		EventId:       event.ID.String(),
		OccurredAt:    occurredAt,
		ProjectName:   rsc.Tenant().ProjectName().String(),
		NamespaceName: rsc.Tenant().NamespaceName().String(),
		EventType:     eventType,
		Payload: &pbInt.OptimusChangeEvent_ResourceChange{
			ResourceChange: &pbInt.ResourceChangePayload{
				DatastoreName: rsc.Store().String(),
				Resource:      resourcePb,
			},
		},
	}
	return proto.Marshal(optEvent)
}
