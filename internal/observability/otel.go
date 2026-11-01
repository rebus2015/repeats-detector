package observability

import (
	"context"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}
func (h traceHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(a)}
}
func (h traceHandler) WithGroup(g string) slog.Handler { return traceHandler{h.Handler.WithGroup(g)} }
func Logger(levels ...slog.Level) *slog.Logger {
	level := slog.LevelInfo
	if len(levels) > 0 {
		level = levels[0]
	}
	return slog.New(traceHandler{slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})})
}

type Metrics struct {
	OutboxPending   metric.Int64Gauge
	OutboxOldest    metric.Float64Gauge
	MonitorErrors   metric.Int64Counter
	Inflight        metric.Int64UpDownCounter
	Retries         metric.Int64Counter
	Processed       metric.Int64Counter
	Duration, Stage metric.Float64Histogram
}

func Setup(ctx context.Context, endpoint string) (*Metrics, func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	shutdown := func(context.Context) error { return nil }
	if os.Getenv("OTEL_SDK_DISABLED") != "true" {
		to := []otlptracegrpc.Option{otlptracegrpc.WithEndpointURL(endpoint)}
		mo := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpointURL(endpoint)}
		if strings.HasPrefix(endpoint, "http://") {
			to = append(to, otlptracegrpc.WithInsecure())
			mo = append(mo, otlpmetricgrpc.WithInsecure())
		}
		te, e := otlptracegrpc.New(ctx, to...)
		if e != nil {
			return nil, shutdown, e
		}
		me, e := otlpmetricgrpc.New(ctx, mo...)
		if e != nil {
			_ = te.Shutdown(ctx)
			return nil, shutdown, e
		}
		res, e := telemetryResource(ctx)
		if e != nil {
			_ = te.Shutdown(ctx)
			_ = me.Shutdown(ctx)
			return nil, shutdown, e
		}
		tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(te), sdktrace.WithResource(res))
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(sdkmetric.NewPeriodicReader(me, metricReaderOptions()...)))
		otel.SetTracerProvider(tp)
		otel.SetMeterProvider(mp)
		shutdown = func(c context.Context) error {
			e := tp.Shutdown(c)
			m := mp.Shutdown(c)
			if e != nil {
				return e
			}
			return m
		}
	}
	meter := otel.Meter("repeats-detector")
	heap, e := meter.Int64ObservableGauge("audio_go_heap_bytes", metric.WithUnit("By"))
	if e != nil {
		return nil, shutdown, e
	}
	goroutines, e := meter.Int64ObservableGauge("audio_go_goroutines")
	if e != nil {
		return nil, shutdown, e
	}
	if _, e = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		o.ObserveInt64(heap, int64(stats.HeapAlloc))
		o.ObserveInt64(goroutines, int64(runtime.NumGoroutine()))
		return nil
	}, heap, goroutines); e != nil {
		return nil, shutdown, e
	}
	m := new(Metrics)
	m.OutboxPending, e = meter.Int64Gauge("audio_outbox_pending")
	if e != nil {
		return nil, shutdown, e
	}
	m.OutboxOldest, e = meter.Float64Gauge("audio_outbox_oldest_seconds", metric.WithUnit("s"))
	if e != nil {
		return nil, shutdown, e
	}
	m.MonitorErrors, e = meter.Int64Counter("audio_monitor_errors_total")
	if e != nil {
		return nil, shutdown, e
	}
	m.Inflight, e = meter.Int64UpDownCounter("audio_inflight")
	if e != nil {
		return nil, shutdown, e
	}
	m.Retries, e = meter.Int64Counter("audio_retries_total")
	if e != nil {
		return nil, shutdown, e
	}
	m.Processed, e = meter.Int64Counter("audio_processed_total")
	if e != nil {
		return nil, shutdown, e
	}
	m.Duration, e = meter.Float64Histogram("audio_processing_duration_seconds", metric.WithUnit("s"))
	if e != nil {
		return nil, shutdown, e
	}
	m.Stage, e = meter.Float64Histogram("audio_stage_duration_seconds", metric.WithUnit("s"))
	return m, shutdown, e
}
func (m *Metrics) Record(ctx context.Context, status string, elapsed time.Duration) {
	a := metric.WithAttributes(attribute.String("status", status))
	m.Processed.Add(ctx, 1, a)
	m.Duration.Record(ctx, elapsed.Seconds(), a)
}
func (m *Metrics) StageTime(ctx context.Context, stage string, start time.Time) {
	m.Stage.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attribute.String("stage", stage)))
}

func telemetryResource(ctx context.Context) (*resource.Resource, error) {
	return resource.New(ctx, resource.WithAttributes(attribute.String("service.name", "repeats-detector")), resource.WithFromEnv())
}

func metricReaderOptions() []sdkmetric.PeriodicReaderOption {
	// Preserve the existing 10-second default; let the SDK parse its standard ENV.
	if os.Getenv("OTEL_METRIC_EXPORT_INTERVAL") == "" {
		return []sdkmetric.PeriodicReaderOption{sdkmetric.WithInterval(10 * time.Second)}
	}
	return nil
}
