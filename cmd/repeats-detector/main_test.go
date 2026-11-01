package main

import (
	"repeats-detector/internal/config"
	"testing"
)

func TestDatabasePoolSizing(t *testing.T) {
	for _, tc := range []struct {
		dsn     string
		workers int
		want    int32
	}{
		{"postgres://test:test@localhost/test", 2, 8},
		{"postgres://test:test@localhost/test?pool_max_conns=17", 2, 17},
	} {
		c, err := databasePoolConfig(config.Config{DatabaseURL: tc.dsn, Workers: tc.workers})
		if err != nil {
			t.Fatal(err)
		}
		if c.MaxConns != tc.want {
			t.Fatalf("MaxConns=%d, want %d", c.MaxConns, tc.want)
		}
	}
}
