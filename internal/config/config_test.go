package config

import (
	"testing"
)

func TestRejectUnsafeConfig(t *testing.T) {
	for _, tt := range []struct{ key, value string }{{"WORKERS", "0"}, {"MIN_MATCH_COVERAGE", "NaN"}, {"MIN_MATCH_SECONDS", "+Inf"}, {"INDEX_RETENTION", "1m"}, {"DATABASE_URL", "http://localhost"}, {"RABBITMQ_URL", "https://localhost"}, {"S3_ENDPOINT", "ftp://localhost"}, {"OUTPUT_QUEUE", "audio.tasks"}} {
		t.Run(tt.key, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://test:test@localhost/test")
			t.Setenv(tt.key, tt.value)
			if _, e := Load(); e == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}
}

func TestTLSRequired(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test:test@db/test?sslmode=verify-full")
	t.Setenv("RABBITMQ_URL", "amqps://test:test@rabbit:5671/")
	t.Setenv("S3_ENDPOINT", "https://minio:9000")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector:4317")
	t.Setenv("REQUIRE_TLS", "true")
	if _, e := Load(); e != nil {
		t.Fatal(e)
	}
	for _, tt := range []struct{ k, v string }{{"DATABASE_URL", "postgres://test:test@db/test?sslmode=require"}, {"RABBITMQ_URL", "amqp://test:test@rabbit:5672/"}, {"S3_ENDPOINT", "http://minio:9000"}, {"OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317"}} {
		t.Run(tt.k, func(t *testing.T) {
			t.Setenv(tt.k, tt.v)
			if _, e := Load(); e == nil {
				t.Fatal("insecure production endpoint accepted")
			}
		})
	}
}
