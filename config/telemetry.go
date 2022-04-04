package config

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/odpf/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

const MetricWaitInterval = time.Second * 2

func InitTelemetry(l log.Logger, conf TelemetryConfig) (func(), error) {
	var tp *tracesdk.TracerProvider
	var err error
	if conf.JaegerAddr != "" {
		l.Debug("enabling jaeger traces", "addr", conf.JaegerAddr)
		tp, err = tracerProvider(conf.JaegerAddr)
		if err != nil {
			return nil, err
		}

		// Register our TracerProvider as the global so any imported
		// instrumentation in the future will default to using it.
		otel.SetTracerProvider(tp)

		// Traces can extend beyond a single process. This requires context propagation, a mechanism where identifiers for a trace are sent to remote processes.
		// TextMapPropagator performs the injection and extraction of a cross-cutting concern value as string key/values
		// pairs into carriers that travel in-band across process boundaries.
		// The carrier of propagated data on both the client (injector) and server (extractor) side is usually an HTTP request.
		// In order to increase compatibility, the key/value pairs MUST only consist of US-ASCII characters that make up
		// valid HTTP header fields as per RFC 7230.
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	}

	var metricServer *http.Server
	if conf.ProfileAddr != "" {
		l.Debug("enabling profile metrics", "addr", conf.ProfileAddr)
		// custom metric for app uptime
		go func() {
			appUptime := promauto.NewGauge(prometheus.GaugeOpts{
				Name: "application_uptime_seconds",
				Help: "Seconds since the application started",
			})
			appHeartbeat := promauto.NewCounter(prometheus.CounterOpts{
				Name: "application_heartbeat",
				Help: "Application heartbeat pings",
			})
			startTime := time.Now()
			for {
				time.Sleep(MetricWaitInterval)
				appUptime.Set(time.Since(startTime).Seconds())
				appHeartbeat.Inc()
			}
		}()

		// start exposing metrics
		metricServer = MetricsServer(conf.ProfileAddr)
		go func() {
			if err := metricServer.ListenAndServe(); err != http.ErrServerClosed {
				l.Warn("failed while serving metrics", "err", err)
			}
		}()
	}
	return func() {
		if tp != nil {
			if err = tp.Shutdown(context.Background()); err != nil {
				l.Warn("failed to shutdown trace provider", "err", err)
			}
		}
		if metricServer != nil {
			if err := metricServer.Close(); err != nil {
				l.Warn("failed to shutdown metrics http server", "err", fmt.Errorf("metricServer.Close: %w", err))
			}
		}
	}, nil
}

// tracerProvider returns an OpenTelemetry TracerProvider configured to use
// the Jaeger exporter that will send spans to the provided url. The returned
// TracerProvider will also use a Resource configured with all the information
// about the application.
func tracerProvider(url string) (*tracesdk.TracerProvider, error) {
	// create the Jaeger exporter
	jaegerExporter, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		return nil, err
	}
	tp := tracesdk.NewTracerProvider(
		// Always be sure to batch in production
		tracesdk.WithBatcher(jaegerExporter),

		// Record information about this application in an Resource
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(AppName()),
			semconv.ServiceVersionKey.String(BuildVersion),
			attribute.String("build_commit", BuildCommit),
			attribute.String("build_date", BuildDate),
		)),
	)

	return tp, nil
}

func MetricsServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}
