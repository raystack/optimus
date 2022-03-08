package mock

import (
	"context"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/metadata"
)

type DeployJobSpecificationServer struct {
	mock.Mock
}

func (r *DeployJobSpecificationServer) Send(response *pb.DeployJobSpecificationResponse) error {
	args := r.Called(response)
	return args.Error(0)
}

func (r *DeployJobSpecificationServer) SetHeader(md metadata.MD) error {
	panic("implement me")
}

func (r *DeployJobSpecificationServer) SendHeader(md metadata.MD) error {
	panic("implement me")
}

func (r *DeployJobSpecificationServer) SetTrailer(md metadata.MD) {
	panic("implement me")
}

func (r *DeployJobSpecificationServer) Context() context.Context {
	args := r.Called()
	return args.Get(0).(context.Context)
}

func (r *DeployJobSpecificationServer) SendMsg(m interface{}) error {
	panic("implement me")
}

func (r *DeployJobSpecificationServer) RecvMsg(m interface{}) error {
	panic("implement me")
}
