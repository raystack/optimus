package connection

import (
	"errors"
	"os"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/salt/log"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/goto/optimus/config"
)

const (
	grpcMaxClientSendSize      = 128 << 20 // 128MB
	grpcMaxClientRecvSize      = 128 << 20 // 128MB
	grpcMaxRetry          uint = 3

	optimusDialTimeout = time.Second * 2
	backoffDuration    = 100 * time.Millisecond
)

var errServerNotReachable = func(host string) error {
	return errors.New(heredoc.Docf(`Unable to reach optimus server at %s, this can happen due to following reasons:
		1. Check if you are connected to internet
		2. Is the host correctly configured in optimus config
		3. Is Optimus server currently unreachable`, host))
}

type Connection interface {
	Create(host string) (*grpc.ClientConn, error)
}

func New(l log.Logger, cfg *config.ClientConfig) Connection {
	if useInsecure() {
		return NewInsecure(l)
	}

	return NewSecure(l, cfg)
}

func useInsecure() bool {
	if insecure := os.Getenv("OPTIMUS_INSECURE"); insecure != "" {
		return true
	}
	return false
}

func defaultDialOptions() []grpc.DialOption {
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(backoffDuration)),
		grpc_retry.WithMax(grpcMaxRetry),
	}
	var opts []grpc.DialOption
	opts = append(opts,
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(grpcMaxClientSendSize),
			grpc.MaxCallRecvMsgSize(grpcMaxClientRecvSize),
		),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			grpc_retry.UnaryClientInterceptor(retryOpts...),
			otelgrpc.UnaryClientInterceptor(),
			grpc_prometheus.UnaryClientInterceptor,
		)),
		grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
			otelgrpc.StreamClientInterceptor(),
			grpc_prometheus.StreamClientInterceptor,
		)),
	)
	return opts
}
