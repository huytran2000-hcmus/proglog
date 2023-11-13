package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

const (
	traceFile = "../../var/otel.json"
)

func InitOtel(ctx context.Context) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error

	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}

		shutdownFuncs = nil
		return err
	}

	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	rsrc, err := newResource(serverName)
	if err != nil {
		handleErr(err)
		return nil, fmt.Errorf("init a resource: %w", err)
	}

	tp, err := newTraceProvider(rsrc)
	if err != nil {
		handleErr(err)
		return nil, fmt.Errorf("init trace provider: %w", err)
	}
	shutdownFuncs = append(shutdownFuncs, func(ctx context.Context) error {
		return errors.Join(tp.ForceFlush(ctx), tp.Shutdown(ctx))
	})

	otel.SetTracerProvider(tp)

	return shutdown, nil
}

func newResource(serviceName string) (*resource.Resource, error) {
	return resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serverName),
		),
	)
}

func newTraceProvider(rsrc *resource.Resource) (*trace.TracerProvider, error) {
	writer, err := getTraceFileWriter(traceFile)
	if err != nil {
		return nil, fmt.Errorf("get trace file writer: %w", err)
	}
	exporter, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
		stdouttrace.WithWriter(writer),
	)
	if err != nil {
		return nil, fmt.Errorf("init trace exporter: %w", err)
	}

	provider := trace.NewTracerProvider(
		trace.WithResource(rsrc),
		trace.WithBatcher(exporter),
		trace.WithSampler(trace.AlwaysSample()),
	)

	return provider, nil
}

func getTraceFileWriter(filename string) (*bufio.Writer, error) {
	err := os.MkdirAll(filepath.Dir(filename), 0755)
	if err != nil {
		return nil, fmt.Errorf("create directory %s: %w", filename, err)
	}

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	writer := bufio.NewWriterSize(file, 1024)

	return writer, nil
}
