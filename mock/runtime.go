package mock

import (
	"context"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/metadata"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
)

type RuntimeService_DeployJobSpecificationServer struct {
	mock.Mock
}

func (r *RuntimeService_DeployJobSpecificationServer) Send(response *pb.DeployJobSpecificationResponse) error {
	args := r.Called(response)
	return args.Error(0)
}

func (r *RuntimeService_DeployJobSpecificationServer) SetHeader(md metadata.MD) error {
	panic("implement me")
}

func (r *RuntimeService_DeployJobSpecificationServer) SendHeader(md metadata.MD) error {
	panic("implement me")
}

func (r *RuntimeService_DeployJobSpecificationServer) SetTrailer(md metadata.MD) {
	panic("implement me")
}

func (r *RuntimeService_DeployJobSpecificationServer) Context() context.Context {
	args := r.Called()
	return args.Get(0).(context.Context)
}

func (r *RuntimeService_DeployJobSpecificationServer) SendMsg(m interface{}) error {
	panic("implement me")
}

func (r *RuntimeService_DeployJobSpecificationServer) RecvMsg(m interface{}) error {
	panic("implement me")
}
