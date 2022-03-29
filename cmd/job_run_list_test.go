package cmd

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
)

func TestGetJobRunList(t *testing.T) {
	t.Log("Testing getJobRunList")
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "test", grpc.WithInsecure(), grpc.WithContextDialer(dialer()))
	if err != nil {
		t.Fatalf("could not able to test getJobRunList %v", err)
	}
	defer conn.Close()
	l := log.NewNoop()
	//t.Logf("address of grpc server %v", address)
	host := "test"
	testCases := []struct {
		description string
		input       jobRunCmdArg
		err         error
	}{
		{
			"when job is not present",
			jobRunCmdArg{
				projectName: "project-test",
				jobName:     "not-present",
			},
			status.Errorf(codes.NotFound, "job not found %v", "not-present"),
		},
		{
			"when job is present",
			jobRunCmdArg{
				projectName: "project-test",
				jobName:     "job-test",
			},
			nil,
		},
	}
	for _, scenario := range testCases {
		t.Run(scenario.description, func(t *testing.T) {
			err := getJobRunList(l, host, scenario.input)
			assert.Equal(t, scenario.err, err)
		})
	}
}

type mockJobRunService struct {
	pb.UnimplementedJobRunServiceServer
}

func (*mockJobRunService) JobRun(_ context.Context, req *pb.JobRunRequest) (*pb.JobRunResponse, error) {
	if req.JobName == "not-present" {
		return nil, status.Errorf(codes.NotFound, "job not found %v", req.GetJobName())
	}
	scheduledTime, err := time.Parse(time.RFC3339, "2022-03-10T02:00:00Z")
	if err != nil {
		return nil, err
	}
	var runs []*pb.JobRun
	runs = append(runs, &pb.JobRun{
		State:       "success",
		ScheduledAt: timestamppb.New(scheduledTime),
	})
	resp := &pb.JobRunResponse{
		JobRuns: runs,
	}
	return resp, nil
}

func dialer() func(context.Context, string) (net.Conn, error) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	pb.RegisterJobRunServiceServer(server, &mockJobRunService{})
	server.GetServiceInfo()
	go func() {
		if err := server.Serve(listener); err != nil {
			log.NewNoop().Fatal("not able to start grpc server to test getJobRunList", err)
		}
	}()

	return func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
}
