package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpctags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/odpf/salt/log"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	_ "github.com/odpf/optimus/ext/datastore"
	"github.com/odpf/optimus/plugin"
)

const (
	shutdownWait       = 30 * time.Second
	GRPCMaxRecvMsgSize = 128 << 20 // 128MB
	GRPCMaxSendMsgSize = 128 << 20 // 128MB

	DialTimeout      = time.Second * 5
	BootstrapTimeout = time.Second * 10
)

func checkRequiredConfigs(conf config.Serve) error {
	errRequiredMissing := errors.New("required config missing")
	if conf.IngressHost == "" {
		return fmt.Errorf("serve.ingress_host: %w", errRequiredMissing)
	}
	if conf.Replay.NumWorkers < 1 {
		return fmt.Errorf("%s should be greater than 0", config.KeyServeReplayNumWorkers)
	}
	if conf.DB.DSN == "" {
		return fmt.Errorf("serve.db.dsn: %w", errRequiredMissing)
	}
	if parsed, err := url.Parse(conf.DB.DSN); err != nil {
		return fmt.Errorf("failed to parse serve.db.dsn: %w", err)
	} else if parsed.Scheme != "postgres" {
		return errors.New("unsupported database scheme, use 'postgres'")
	}
	return nil
}

func setupGRPCServer(l log.Logger) (*grpc.Server, error) {
	// Logrus entry is used, allowing pre-definition of certain fields by the user.
	grpcLogLevel, err := logrus.ParseLevel(l.Level())
	if err != nil {
		return nil, err
	}
	grpcLogrus := logrus.New()
	grpcLogrus.SetLevel(grpcLogLevel)
	grpcLogrusEntry := logrus.NewEntry(grpcLogrus)
	// Shared options for the logger, with a custom gRPC code to log level function.
	opts := []grpc_logrus.Option{
		grpc_logrus.WithLevels(grpc_logrus.DefaultCodeToLevel),
	}
	// Make sure that log statements internal to gRPC library are logged using the logrus logger as well.
	grpc_logrus.ReplaceGrpcLogger(grpcLogrusEntry)

	recoverPanic := func(p interface{}) (err error) {
		return status.Error(codes.Unknown, fmt.Sprintf("panic is triggered: %v", p))
	}
	grpcOpts := []grpc.ServerOption{
		grpc_middleware.WithUnaryServerChain(
			grpctags.UnaryServerInterceptor(grpctags.WithFieldExtractor(grpctags.CodeGenRequestFieldExtractor)),
			grpc_logrus.UnaryServerInterceptor(grpcLogrusEntry, opts...),
			otelgrpc.UnaryServerInterceptor(),
			grpc_prometheus.UnaryServerInterceptor,
			grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandler(recoverPanic)),
		),
		grpc_middleware.WithStreamServerChain(
			otelgrpc.StreamServerInterceptor(),
			grpc_prometheus.StreamServerInterceptor,
			grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandler(recoverPanic)),
		),
		grpc.MaxRecvMsgSize(GRPCMaxRecvMsgSize),
		grpc.MaxSendMsgSize(GRPCMaxSendMsgSize),
	}
	grpcServer := grpc.NewServer(grpcOpts...)
	reflection.Register(grpcServer)
	return grpcServer, nil
}

func prepareHTTPProxy(grpcAddr string, grpcServer *grpc.Server) (*http.Server, func(), error) {
	timeoutGrpcDialCtx, grpcDialCancel := context.WithTimeout(context.Background(), DialTimeout)
	defer grpcDialCancel()

	// prepare http proxy
	gwmux := runtime.NewServeMux(
		runtime.WithErrorHandler(runtime.DefaultHTTPErrorHandler),
	)
	// gRPC dialup options to proxy http connections
	grpcConn, err := grpc.DialContext(timeoutGrpcDialCtx, grpcAddr, []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(GRPCMaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(GRPCMaxSendMsgSize),
		),
	}...)
	if err != nil {
		return nil, func() {}, fmt.Errorf("grpc.DialContext: %w", err)
	}
	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	cleanup := func() {
		runtimeCancel()
	}

	if err := pb.RegisterRuntimeServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterRuntimeServiceHandler: %w", err)
	}
	if err := pb.RegisterJobSpecificationServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterJobSpecificationServiceHandler: %w", err)
	}
	if err := pb.RegisterJobRunServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterJobRunServiceHandler: %w", err)
	}
	if err := pb.RegisterProjectServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterProjectServiceHandler: %w", err)
	}
	if err := pb.RegisterNamespaceServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterNamespaceServiceHandler: %w", err)
	}
	if err := pb.RegisterReplayServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterReplayServiceHandler: %w", err)
	}
	if err := pb.RegisterBackupServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterBackupServiceHandler: %w", err)
	}
	if err := pb.RegisterResourceServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterResourceServiceHandler: %w", err)
	}
	if err := pb.RegisterSecretServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterSecretServiceHandler: %w", err)
	}

	// base router
	baseMux := http.NewServeMux()
	baseMux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "pong")
	})
	baseMux.HandleFunc("/plugins", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/zip")
		http.ServeFile(w, r, plugin.PluginsArchiveName)
	})
	baseMux.Handle("/api/", otelhttp.NewHandler(http.StripPrefix("/api", gwmux), "api"))

	//nolint: gomnd
	srv := &http.Server{
		Handler:      grpcHandlerFunc(grpcServer, baseMux),
		Addr:         grpcAddr,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	return srv, cleanup, nil
}

// grpcHandlerFunc routes http1 calls to baseMux and http2 with grpc header to grpcServer.
// Using a single port for proxying both http1 & 2 protocols will degrade http performance
// but for our use-case the convenience per performance tradeoff is better suited
// if in the future, this does become a bottleneck(which I highly doubt), we can break the service
// into two ports, default port for grpc and default+1 for grpc-gateway proxy.
// We can also use something like a connection multiplexer
// https://github.com/soheilhy/cmux to achieve the same.
func grpcHandlerFunc(grpcServer *grpc.Server, otherHandler http.Handler) http.Handler {
	return h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
		} else {
			otherHandler.ServeHTTP(w, r)
		}
	}), &http2.Server{})
}
