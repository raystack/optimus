package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/jinzhu/gorm"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	v1handler "github.com/odpf/optimus/api/handler/v1"
	pb "github.com/odpf/optimus/api/proto/v1"
	"github.com/odpf/optimus/core/logger"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/postgres"
)

var (
	// Version of the service
	// overridden by the build system. see "Makefile"
	Version = ""

	// AppName is used to prefix Version
	AppName = "optimus"

	//listen for sigterm
	termChan = make(chan os.Signal, 1)

	shutdownWait = 30 * time.Second
)

// Config for the service
var Config = struct {
	ServerPort string
	ServerHost string
	LogLevel   string
	DBURL      string
}{
	ServerPort: "8080",
	ServerHost: "0.0.0.0",
	LogLevel:   "DEBUG",
}

func lookupEnvOrString(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// cfg defines an input parameter to the service
type cfg struct {
	Env, Cmd, Desc string
}

// cfgRules define how input parameters map to local
// configuration variables
var cfgRules = map[*string]cfg{
	&Config.ServerPort: {
		Env:  "SERVER_PORT",
		Cmd:  "server-port",
		Desc: "port to listen on",
	},
	&Config.ServerHost: {
		Env:  "SERVER_HOST",
		Cmd:  "server-host",
		Desc: "the network interface to listen on",
	},
	&Config.LogLevel: {
		Env:  "LOG_LEVEL",
		Cmd:  "log-level",
		Desc: "log level - DEBUG, INFO, WARNING, ERROR, FATAL",
	},
	&Config.DBURL: {
		Env:  "DB_URL",
		Cmd:  "db-url",
		Desc: "database connection url",
	},
}

func validateConfig() error {
	var errs []string
	for v, cfg := range cfgRules {
		if strings.TrimSpace(*v) == "" {
			errs = append(
				errs,
				fmt.Sprintf(
					"missing required parameter: -%s (can also be set using %s environment variable)",
					cfg.Cmd,
					cfg.Env,
				),
			)
		}
		if *v == "-" { // "- is used for empty arguments"
			*v = ""
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "\n"))
	}
	return nil
}

type jobSpecRepoFactory struct {
	db *gorm.DB
}

func (fac *jobSpecRepoFactory) New(proj models.ProjectSpec) store.JobSpecRepository {
	return postgres.NewJobRepository(fac.db, proj)
}

func init() {
	for v, cfg := range cfgRules {
		flag.StringVar(v, cfg.Cmd, lookupEnvOrString(cfg.Env, *v), cfg.Desc)
	}
	flag.Parse()
}

func main() {

	log := logrus.New()
	log.SetOutput(os.Stdout)

	mainLog := log.WithField("reporter", "main")
	mainLog.Infof("starting optimus %s", Version)

	err := validateConfig()
	if err != nil {
		mainLog.Fatalf("configuration error:\n%v", err)
	}

	logger.Init(Config.LogLevel)

	//setup routes
	httpRouter := mux.NewRouter()

	// create the http handlers
	httpRouter.Path("/ping").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "pong")
	})
	httpRouter.Path("/version").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, Version)
	})

	// start the server
	addr := fmt.Sprintf("%s:%s", Config.ServerHost, Config.ServerPort)
	mainLog.Infof("listening on %s", addr)
	srv := &http.Server{
		Handler: httpRouter,
		Addr:    addr,
	}

	// Run our server in a goroutine so that it doesn't block.
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				mainLog.Fatalf("server error: %v\n", err)
			}
		}
	}()

	//grpc server
	grpcAddr := fmt.Sprintf("%s:%s", Config.ServerHost, Config.ServerPort)

	//create a tcp listener for grpc
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcOpts := []grpc.ServerOption{}
	grpcServer := grpc.NewServer(grpcOpts...)
	reflection.Register(grpcServer)

	if err := postgres.Migrate(Config.DBURL); err != nil {
		panic(err)
	}
	dbConn, err := postgres.Connect(Config.DBURL, 5, 10)
	if err != nil {
		panic(err)
	}

	// runtime service instance over gprc
	pb.RegisterRuntimeServiceServer(grpcServer, v1handler.NewRuntimeServiceServer(
		Version,
		job.NewService(
			&jobSpecRepoFactory{
				db: dbConn,
			},
		),
	))

	// start grpc server
	go func() {
		log.Fatal(grpcServer.Serve(lis))
	}()

	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	signal.Notify(termChan, os.Interrupt)
	signal.Notify(termChan, os.Kill)
	signal.Notify(termChan, syscall.SIGTERM)

	// Block until we receive our signal.
	<-termChan
	mainLog.Info("termination request received")

	// Create a deadline to wait for server
	ctx, cancel := context.WithTimeout(context.Background(), shutdownWait)
	defer cancel()

	// Doesn't block if no connections, but will otherwise wait
	// until the timeout deadline.
	if err := srv.Shutdown(ctx); err != nil {
		mainLog.Warn(err)
	}

	grpcServer.GracefulStop()

	mainLog.Info("bye")
}
