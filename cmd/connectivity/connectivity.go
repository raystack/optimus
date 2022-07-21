package connectivity

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"time"

	"github.com/MakeNowJust/heredoc"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
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

// Connectivity defines client connection to a targeted server host
type Connectivity struct {
	requestCtx       context.Context //nolint:containedctx
	cancelRequestCtx func()

	connection *grpc.ClientConn
}

// NewConnectivity initializes client connection
func NewConnectivity(serverHost string, requestTimeout time.Duration) (*Connectivity, error) {
	connection, err := createConnection(serverHost)
	if err != nil {
		return nil, err
	}
	reqCtx, reqCancel := context.WithTimeout(context.Background(), requestTimeout)
	return &Connectivity{
		requestCtx:       reqCtx,
		cancelRequestCtx: reqCancel,
		connection:       connection,
	}, nil
}

// GetContext gets request context
func (c *Connectivity) GetContext() context.Context {
	return c.requestCtx
}

// GetConnection gets client connection
func (c *Connectivity) GetConnection() *grpc.ClientConn {
	return c.connection
}

// Close closes client connection and its context
func (c *Connectivity) Close() {
	c.connection.Close()
	c.cancelRequestCtx()
}

func createConnection(host string) (*grpc.ClientConn, error) {
	opts := getDefaultDialOptions()

	// pass rpc credentials
	if token := os.Getenv("OPTIMUS_AUTH_BASIC_TOKEN"); token != "" {
		base64Token := base64.StdEncoding.EncodeToString([]byte(token))
		opts = append(opts, grpc.WithPerRPCCredentials(&basicAuthentication{
			Token: base64Token,
		}))
	} else if token := os.Getenv("OPTIMUS_AUTH_BEARER_TOKEN"); token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(&bearerAuthentication{
			Token: token,
		}))
	}

	ctx, dialCancel := context.WithTimeout(context.Background(), optimusDialTimeout)
	conn, err := grpc.DialContext(ctx, host, opts...)
	if errors.Is(err, context.DeadlineExceeded) {
		err = errServerNotReachable(host)
	}
	dialCancel()
	return conn, err
}

func getDefaultDialOptions() []grpc.DialOption {
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(backoffDuration)),
		grpc_retry.WithMax(grpcMaxRetry),
	}
	var opts []grpc.DialOption
	opts = append(opts,
		grpc.WithInsecure(),
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
