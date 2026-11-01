package config

import (
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"repeats-detector/internal/matching"
)

type Config struct {
	AutoMigrate                                                                                           bool
	RequireTLS                                                                                            bool
	DatabaseURL, RabbitURL, InputQueue, OutputQueue, DLQueue, HTTPAddr, OTLPEndpoint, S3Endpoint          string
	Workers, MaxBytes, MaxPrints, MaxHits, MaxPostings                                                    int
	TaskTimeout, PublishTimeout, ShutdownTimeout, SearchWindow, Retention, CleanupInterval, TaskRetention time.Duration
	RabbitConnectTimeout, RabbitHeartbeat, RabbitReconnectDelay, TaskRetryDelay                           time.Duration
	OutboxPollInterval, OutboxRetryDelay, DeliveryPollInterval, StartupTimeout, CleanupTimeout            time.Duration
	TaskDeliveryLimit, CleanupPrintsBatchSize, CleanupTasksBatchSize                                      int
	LogLevel                                                                                              slog.Level
	Match                                                                                                 matching.Config
}

func Load() (Config, error) {
	c := Config{DatabaseURL: os.Getenv("DATABASE_URL"), RabbitURL: env("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"), InputQueue: env("INPUT_QUEUE", "audio.tasks"), OutputQueue: env("OUTPUT_QUEUE", "audio.results"), DLQueue: env("DLQ_QUEUE", "audio.tasks.dead"), HTTPAddr: env("HTTP_ADDR", ":8080"), OTLPEndpoint: env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"), S3Endpoint: os.Getenv("S3_ENDPOINT"), Match: matching.Default()}
	var err error
	c.AutoMigrate, err = strconv.ParseBool(env("AUTO_MIGRATE", "true"))
	if err != nil {
		return c, fmt.Errorf("invalid AUTO_MIGRATE")
	}
	c.RequireTLS, err = strconv.ParseBool(env("REQUIRE_TLS", "false"))
	if err != nil {
		return c, fmt.Errorf("invalid REQUIRE_TLS")
	}
	for _, v := range []struct {
		name string
		dst  *int
		def  int
	}{
		{"TASK_DELIVERY_LIMIT", &c.TaskDeliveryLimit, 5},
		{"CLEANUP_PRINTS_BATCH_SIZE", &c.CleanupPrintsBatchSize, 50000},
		{"CLEANUP_TASKS_BATCH_SIZE", &c.CleanupTasksBatchSize, 1000},
		{"MIN_MATCH_FRAMES", &c.Match.MinFrames, 12},
		{"WORKERS", &c.Workers, 2},
		{"MAX_WAV_BYTES", &c.MaxBytes, 128000000},
		{"MAX_FINGERPRINTS", &c.MaxPrints, 1000000},
		{"MAX_SEARCH_HITS", &c.MaxHits, 200000},
		{"MAX_HASH_POSTINGS", &c.MaxPostings, 32},
	} {
		if *v.dst, err = integer(v.name, v.def); err != nil {
			return c, err
		}
	}
	for _, v := range []struct {
		name string
		dst  *time.Duration
		def  string
	}{
		{"RABBITMQ_CONNECT_TIMEOUT", &c.RabbitConnectTimeout, "10s"},
		{"RABBITMQ_HEARTBEAT", &c.RabbitHeartbeat, "10s"},
		{"RABBITMQ_RECONNECT_DELAY", &c.RabbitReconnectDelay, "2s"},
		{"TASK_RETRY_DELAY", &c.TaskRetryDelay, "1s"},
		{"OUTBOX_POLL_INTERVAL", &c.OutboxPollInterval, "100ms"},
		{"OUTBOX_RETRY_DELAY", &c.OutboxRetryDelay, "2s"},
		{"DELIVERY_POLL_INTERVAL", &c.DeliveryPollInterval, "100ms"},
		{"STARTUP_TIMEOUT", &c.StartupTimeout, "30s"},
		{"CLEANUP_TIMEOUT", &c.CleanupTimeout, "30s"},
		{"TASK_TIMEOUT", &c.TaskTimeout, "2m"},
		{"PUBLISH_TIMEOUT", &c.PublishTimeout, "10s"},
		{"SHUTDOWN_TIMEOUT", &c.ShutdownTimeout, "30s"},
		{"SEARCH_WINDOW", &c.SearchWindow, "60m"},
		{"INDEX_RETENTION", &c.Retention, "90m"},
		{"CLEANUP_INTERVAL", &c.CleanupInterval, "5m"},
		{"TASK_RETENTION", &c.TaskRetention, "168h"},
	} {
		if *v.dst, err = time.ParseDuration(env(v.name, v.def)); err != nil || *v.dst <= 0 {
			return c, fmt.Errorf("invalid %s", v.name)
		}
	}
	for _, v := range []struct {
		name string
		dst  *float64
		def  string
	}{
		{"MIN_MATCH_SECONDS", &c.Match.MinDuration, "2"},
		{"MIN_MATCH_COVERAGE", &c.Match.MinCoverage, "0.08"},
		{"MAX_MATCH_GAP_SECONDS", &c.Match.MaxGap, "0.8"},
	} {
		if *v.dst, err = strconv.ParseFloat(env(v.name, v.def), 64); err != nil || math.IsInf(*v.dst, 0) || math.IsNaN(*v.dst) || !(*v.dst > 0) {
			return c, fmt.Errorf("invalid %s", v.name)
		}
	}
	if err = c.LogLevel.UnmarshalText([]byte(env("LOG_LEVEL", "INFO"))); err != nil {
		return c, fmt.Errorf("invalid LOG_LEVEL")
	}
	if c.TaskDeliveryLimit > 1000 || c.CleanupPrintsBatchSize > 1000000 || c.CleanupTasksBatchSize > 100000 || c.RabbitHeartbeat < time.Second || c.RabbitHeartbeat > 65535*time.Second || c.RabbitHeartbeat%time.Second != 0 {
		return c, fmt.Errorf("operational configuration outside supported bounds")
	}
	if c.Match.MinCoverage > 1 || c.Retention < c.SearchWindow || c.TaskRetention < c.Retention || c.Workers > 64 || c.MaxBytes > 256000000 || c.MaxPrints > 4000000 || c.MaxHits > 1000000 || c.MaxPostings > 10000 {
		return c, fmt.Errorf("configuration outside supported bounds")
	}
	if c.DatabaseURL == "" {
		return c, fmt.Errorf("DATABASE_URL required")
	}
	for name, value := range map[string]string{"database": c.DatabaseURL, "rabbitmq": c.RabbitURL} {
		u, err := url.Parse(value)
		valid := err == nil && u.Host != ""
		if valid && name == "database" {
			valid = u.Scheme == "postgres" || u.Scheme == "postgresql"
		}
		if valid && name == "rabbitmq" {
			valid = u.Scheme == "amqp" || u.Scheme == "amqps"
		}
		if !valid {
			return c, fmt.Errorf("invalid %s connection URL", name)
		}
	}
	if c.S3Endpoint != "" {
		u, err := url.Parse(c.S3Endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return c, fmt.Errorf("invalid S3_ENDPOINT")
		}
	}
	if u, err := url.Parse(c.OTLPEndpoint); err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return c, fmt.Errorf("invalid OTLP endpoint: use http://host:4317 or https://host:4317")
	}

	if c.RequireTLS {
		db, _ := url.Parse(c.DatabaseURL)
		rabbit, _ := url.Parse(c.RabbitURL)
		if db.Query().Get("sslmode") != "verify-full" || rabbit.Scheme != "amqps" ||
			(c.S3Endpoint != "" && !strings.HasPrefix(c.S3Endpoint, "https://")) || !strings.HasPrefix(c.OTLPEndpoint, "https://") {
			return c, fmt.Errorf("REQUIRE_TLS needs PostgreSQL verify-full, amqps and HTTPS S3/OTLP endpoints")
		}
	}

	if c.InputQueue == c.OutputQueue || c.DLQueue == c.InputQueue || c.OutputQueue == c.DLQueue {
		return c, fmt.Errorf("queues must differ")
	}
	return c, nil
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func integer(k string, d int) (int, error) {
	v, e := strconv.Atoi(env(k, strconv.Itoa(d)))
	if e != nil || v <= 0 {
		return 0, fmt.Errorf("invalid %s", k)
	}
	return v, nil
}

// Value returns an environment setting or its default. CLI flags can use it
// as their initial value, so an explicit flag always takes precedence.
func Value(key, fallback string) string { return env(key, fallback) }
