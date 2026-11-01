package observability

import (
	"context"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

func TestResourceEnvironmentOverridesDefault(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "configured-service")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment.name=staging,service.name=attribute-service")
	res, err := telemetryResource(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	attrs := res.Set()
	for key, want := range map[attribute.Key]string{"service.name": "configured-service", "deployment.environment.name": "staging"} {
		got, ok := attrs.Value(key)
		if !ok || got.AsString() != want {
			t.Fatalf("%s=%v, want %s", key, got, want)
		}
	}
}

func TestLoggerLevel(t *testing.T) {
	ctx := context.Background()
	if Logger().Enabled(ctx, slog.LevelDebug) {
		t.Fatal("default logger enables debug")
	}
	if !Logger(slog.LevelDebug).Enabled(ctx, slog.LevelDebug) {
		t.Fatal("configured debug level ignored")
	}
	if Logger(slog.LevelError).Enabled(ctx, slog.LevelInfo) {
		t.Fatal("configured error level ignored")
	}
}
