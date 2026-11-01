package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"

	"repeats-detector/internal/config"
	"repeats-detector/internal/fingerprint"
	"repeats-detector/internal/observability"
	"repeats-detector/internal/source"
	"repeats-detector/internal/store"
	"repeats-detector/internal/usecase"
	"repeats-detector/internal/worker"
)

func main() {
	if e := run(); e != nil {
		observability.Logger().Error("service stopped", "error", e)
		os.Exit(1)
	}
}
func run() error {
	c, e := config.Load()
	if e != nil {
		return e
	}
	log := observability.Logger(c.LogLevel)
	intake, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(intake, c.StartupTimeout)
	defer cancel()
	metrics, shutdown, e := observability.Setup(startup, c.OTLPEndpoint)
	if e != nil {
		return e
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := shutdown(ctx); err != nil {
			log.Error("telemetry shutdown failed", "error", err)
		}
	}()
	pc, e := databasePoolConfig(c)
	if e != nil {
		return fmt.Errorf("database configuration invalid")
	}

	pool, e := pgxpool.NewWithConfig(startup, pc)
	if e != nil {
		return e
	}
	defer pool.Close()
	if e = pool.Ping(startup); e != nil {
		return fmt.Errorf("database unavailable: %w", e)
	}
	repo := &store.Store{Pool: pool, Window: c.SearchWindow, MaxHits: c.MaxHits, MaxPostings: c.MaxPostings, Match: c.Match, CleanupPrintsBatchSize: c.CleanupPrintsBatchSize, CleanupTasksBatchSize: c.CleanupTasksBatchSize}
	if c.AutoMigrate {
		e = repo.Migrate(startup)
	} else {
		e = repo.CheckSchema(startup)
	}
	if e != nil {
		return fmt.Errorf("migrate: %w", e)
	}
	ac, e := awsconfig.LoadDefaultConfig(startup)
	if e != nil {
		return e
	}
	client := s3.NewFromConfig(ac, func(o *s3.Options) {
		if c.S3Endpoint != "" {
			o.BaseEndpoint = &c.S3Endpoint
			o.UsePathStyle = true
		}
	})
	extractor := fingerprint.New()
	extractor.MaxPrints = c.MaxPrints
	processor := &usecase.Processor{Source: &source.S3{Client: client, MaxBytes: c.MaxBytes}, Extractor: extractor, Store: repo, MaxPrints: c.MaxPrints, Metrics: metrics, DeliveryPollInterval: c.DeliveryPollInterval}
	background, halt := context.WithCancel(context.Background())
	defer halt()
	publisher := worker.NewPublisher(c)
	defer publisher.Close()
	outboxDone := make(chan struct{})
	go func() {
		defer close(outboxDone)
		for background.Err() == nil {
			ctx, cancel := context.WithTimeout(background, c.PublishTimeout+5*time.Second)
			worked, err := repo.PublishOne(ctx, publisher.Publish)
			cancel()
			if err != nil {
				log.Error("outbox publication failed", "error", err)
			}
			delay := c.OutboxPollInterval
			if err != nil {
				delay = c.OutboxRetryDelay
			}
			if worked && err == nil {
				continue
			}
			timer := time.NewTimer(delay)
			select {
			case <-background.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
	}()
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		ticker := time.NewTicker(c.CleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-background.Done():
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(background, c.CleanupTimeout)
				err := repo.Cleanup(ctx, c.Retention, c.TaskRetention)
				cancel()
				if err != nil {
					log.Error("retention failed", "error", err)
				}
			}
		}
	}()
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			ctx, cancel := context.WithTimeout(background, 3*time.Second)
			pending, age, err := repo.Backlog(ctx)
			if err == nil {
				metrics.OutboxPending.Record(ctx, pending)
				metrics.OutboxOldest.Record(ctx, age)
			} else if background.Err() == nil {
				metrics.MonitorErrors.Add(ctx, 1)
				log.Error("backlog monitor failed", "error", err)
			}
			cancel()
			select {
			case <-background.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	consumer := worker.Consumer{Config: c, Processor: processor, Log: log, Metrics: metrics}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if intake.Err() != nil || !consumer.Ready.Load() || pool.Ping(ctx) != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Addr: c.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	httpDone := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server failed", "error", err)
			stop()
		}
		httpDone <- err
	}()
	log.Info("service started", "algorithm", fingerprint.Version, "workers", c.Workers)
	e = consumer.Run(intake)
	halt()
	<-outboxDone
	<-cleanupDone
	<-monitorDone
	ctx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	_ = server.Shutdown(ctx)
	if err := <-httpDone; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return e
}

func databasePoolConfig(c config.Config) (*pgxpool.Config, error) {
	pc, err := pgxpool.ParseConfig(c.DatabaseURL)
	if err != nil {
		return nil, err
	}
	// Keep the historical default while respecting explicit DSN pool sizing.
	u, err := url.Parse(c.DatabaseURL)
	if err != nil {
		return nil, err
	}
	if !u.Query().Has("pool_max_conns") {
		pc.MaxConns = int32(c.Workers + 6)
	}
	return pc, nil
}
