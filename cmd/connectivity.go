package cmd

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/mattn/go-isatty"
	"github.com/spf13/afero"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/odpf/optimus/config"
)

const (
	GRPCMaxClientSendSize      = 64 << 20 // 64MB
	GRPCMaxClientRecvSize      = 64 << 20 // 64MB
	GRPCMaxRetry          uint = 3

	OptimusDialTimeout = time.Second * 2
	BackoffDuration    = 100 * time.Millisecond
)

const (
	defaultProjectName = "sample_project"
	defaultHost        = "localhost:9100"
)

type BearerAuthentication struct {
	Token string
}

func (a *BearerAuthentication) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", a.Token),
	}, nil
}

func (a *BearerAuthentication) RequireTransportSecurity() bool {
	return false
}

type BasicAuthentication struct {
	Token string
}

func (a *BasicAuthentication) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{
		"Authorization": fmt.Sprintf("Basic %s", a.Token),
	}, nil
}

func (a *BasicAuthentication) RequireTransportSecurity() bool {
	return false
}

func isTerminal(f *os.File) bool {
	return isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd())
}

func createDataStoreSpecFs(namespace *config.Namespace) map[string]afero.Fs {
	dtSpec := make(map[string]afero.Fs)
	for _, dsConfig := range namespace.Datastore {
		dtSpec[dsConfig.Type] = afero.NewBasePathFs(afero.NewOsFs(), dsConfig.Path)
	}
	return dtSpec
}

func initClientConnection(serverHost string, requestTimeout time.Duration) (
	requestCtx context.Context,
	connection *grpc.ClientConn,
	closeConnection func(),
	err error,
) {
	connection, err = createConnection(serverHost)
	if err != nil {
		return
	}
	reqCtx, reqCancel := context.WithTimeout(context.Background(), requestTimeout)

	requestCtx = reqCtx
	closeConnection = func() {
		connection.Close()
		reqCancel()
	}
	return
}

func createConnection(host string) (*grpc.ClientConn, error) {
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(BackoffDuration)),
		grpc_retry.WithMax(GRPCMaxRetry),
	}
	var opts []grpc.DialOption
	opts = append(opts,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(GRPCMaxClientSendSize),
			grpc.MaxCallRecvMsgSize(GRPCMaxClientRecvSize),
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

	// pass rpc credentials
	if token := os.Getenv("OPTIMUS_AUTH_BASIC_TOKEN"); token != "" {
		base64Token := base64.StdEncoding.EncodeToString([]byte(token))
		opts = append(opts, grpc.WithPerRPCCredentials(&BasicAuthentication{
			Token: base64Token,
		}))
	} else if token := os.Getenv("OPTIMUS_AUTH_BEARER_TOKEN"); token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(&BearerAuthentication{
			Token: token,
		}))
	}

	ctx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	conn, err := grpc.DialContext(ctx, host, opts...)
	if errors.Is(err, context.DeadlineExceeded) {
		err = ErrServerNotReachable(host)
	}
	dialCancel()
	return conn, err
}
