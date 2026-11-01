package config

import (
	"log/slog"
	"testing"
	"time"
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

func TestOperationalSettings(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test:test@localhost/test")
	values := map[string]string{
		"RABBITMQ_CONNECT_TIMEOUT": "7s", "RABBITMQ_HEARTBEAT": "12s", "RABBITMQ_RECONNECT_DELAY": "300ms",
		"TASK_DELIVERY_LIMIT": "9", "TASK_RETRY_DELAY": "250ms", "OUTBOX_POLL_INTERVAL": "150ms", "OUTBOX_RETRY_DELAY": "4s",
		"DELIVERY_POLL_INTERVAL": "50ms", "STARTUP_TIMEOUT": "45s", "CLEANUP_TIMEOUT": "20s",
		"CLEANUP_PRINTS_BATCH_SIZE": "1234", "CLEANUP_TASKS_BATCH_SIZE": "42", "LOG_LEVEL": "DEBUG",
	}
	for key, value := range values {
		t.Setenv(key, value)
	}
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.RabbitConnectTimeout != 7*time.Second || c.RabbitHeartbeat != 12*time.Second || c.RabbitReconnectDelay != 300*time.Millisecond || c.TaskDeliveryLimit != 9 || c.TaskRetryDelay != 250*time.Millisecond || c.OutboxPollInterval != 150*time.Millisecond || c.OutboxRetryDelay != 4*time.Second || c.DeliveryPollInterval != 50*time.Millisecond || c.StartupTimeout != 45*time.Second || c.CleanupTimeout != 20*time.Second || c.CleanupPrintsBatchSize != 1234 || c.CleanupTasksBatchSize != 42 || c.LogLevel != slog.LevelDebug {
		t.Fatalf("settings not loaded: %+v", c)
	}
}

func TestRejectInvalidOperationalSettings(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"TASK_DELIVERY_LIMIT", "0"}, {"TASK_DELIVERY_LIMIT", "1001"}, {"CLEANUP_PRINTS_BATCH_SIZE", "0"},
		{"CLEANUP_TASKS_BATCH_SIZE", "-1"}, {"OUTBOX_POLL_INTERVAL", "0s"}, {"DELIVERY_POLL_INTERVAL", "invalid"},
		{"RABBITMQ_HEARTBEAT", "500ms"}, {"RABBITMQ_HEARTBEAT", "1500ms"}, {"RABBITMQ_HEARTBEAT", "24h"}, {"LOG_LEVEL", "verbose"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://test:test@localhost/test")
			t.Setenv(tc.key, tc.value)
			if _, err := Load(); err == nil {
				t.Fatal("accepted invalid setting")
			}
		})
	}
}
