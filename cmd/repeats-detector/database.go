package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"repeats-detector/internal/matching"
	"repeats-detector/internal/store"
)

func openStore(ctx context.Context, dsn, version string, migrate bool) (*store.Store, error) {
	if dsn == "" {
		return nil, errors.New("задайте DATABASE_URL для подключения к PostgreSQL")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("некорректный DATABASE_URL")
	}
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(startup, config)
	if err != nil {
		return nil, err
	}
	repo := &store.Store{Pool: pool, AlgorithmVersion: version, MaxHits: 200_000, MaxPostings: 32, Match: matching.Default()}
	if migrate {
		err = repo.Migrate(startup)
	} else {
		err = repo.CheckSchema(startup)
	}
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("подготовка PostgreSQL: %w", err)
	}
	return repo, nil
}
