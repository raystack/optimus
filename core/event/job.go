package event

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/job/handler/v1beta1"
	"github.com/goto/optimus/core/tenant"
	pbInt "github.com/goto/optimus/protos/gotocompany/optimus/integration/v1beta1"
)

type JobCreated struct {
	Event

	Job *job.Job
}

func NewJobCreatedEvent(job *job.Job) (*JobCreated, error) {
	baseEvent, err := NewBaseEvent()
	if err != nil {
		return nil, err
	}
	return &JobCreated{
		Event: baseEvent,
		Job:   job,
	}, nil
}

func (j *JobCreated) Bytes() ([]byte, error) {
	return jobEventToBytes(j.Event, j.Job, pbInt.OptimusChangeEvent_EVENT_TYPE_JOB_CREATE)
}

type JobUpdated struct {
	Event

	Job *job.Job
}

func NewJobUpdateEvent(job *job.Job) (*JobUpdated, error) {
	baseEvent, err := NewBaseEvent()
	if err != nil {
		return nil, err
	}
	return &JobUpdated{
		Event: baseEvent,
		Job:   job,
	}, nil
}

func (j *JobUpdated) Bytes() ([]byte, error) {
	return jobEventToBytes(j.Event, j.Job, pbInt.OptimusChangeEvent_EVENT_TYPE_JOB_UPDATE)
}

type JobDeleted struct {
	Event

	JobName   job.Name
	JobTenant tenant.Tenant
}

func NewJobDeleteEvent(tnnt tenant.Tenant, jobName job.Name) (*JobDeleted, error) {
	baseEvent, err := NewBaseEvent()
	if err != nil {
		return nil, err
	}
	return &JobDeleted{
		Event:     baseEvent,
		JobName:   jobName,
		JobTenant: tnnt,
	}, nil
}

func (j *JobDeleted) Bytes() ([]byte, error) {
	occurredAt := timestamppb.New(j.Event.OccurredAt)
	optEvent := &pbInt.OptimusChangeEvent{
		EventId:       j.Event.ID.String(),
		OccurredAt:    occurredAt,
		ProjectName:   j.JobTenant.ProjectName().String(),
		NamespaceName: j.JobTenant.NamespaceName().String(),
		EventType:     pbInt.OptimusChangeEvent_EVENT_TYPE_JOB_DELETE,
		Payload: &pbInt.OptimusChangeEvent_JobChange{
			JobChange: &pbInt.JobChangePayload{
				JobName: j.JobName.String(),
			},
		},
	}
	return proto.Marshal(optEvent)
}

func jobEventToBytes(event Event, job *job.Job, eventType pbInt.OptimusChangeEvent_EventType) ([]byte, error) {
	jobPb := v1beta1.ToJobProto(job)
	occurredAt := timestamppb.New(event.OccurredAt)
	optEvent := &pbInt.OptimusChangeEvent{
		EventId:       event.ID.String(),
		OccurredAt:    occurredAt,
		ProjectName:   job.Tenant().ProjectName().String(),
		NamespaceName: job.Tenant().NamespaceName().String(),
		EventType:     eventType,
		Payload: &pbInt.OptimusChangeEvent_JobChange{
			JobChange: &pbInt.JobChangePayload{
				JobName: job.GetName(),
				JobSpec: jobPb,
			},
		},
	}
	return proto.Marshal(optEvent)
}
