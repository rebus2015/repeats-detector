// search-plan explains a real candidate query using the service SQL and WAV features.
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"repeats-detector/internal/audio"
	"repeats-detector/internal/config"
	"repeats-detector/internal/fingerprint"
	"repeats-detector/internal/store"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	scope := flag.String("scope", "", "existing test scope")
	wav := flag.String("wav", "", "PCM WAV query")
	flag.Parse()
	if *scope == "" || *wav == "" {
		return fmt.Errorf("-scope and -wav required")
	}
	c, err := config.Load()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	data, e := os.ReadFile(*wav)
	if e != nil {
		return e
	}
	pcm, e := audio.Parse(data)
	if e != nil {
		return e
	}
	if len(pcm) > 16000*2*10 {
		pcm = pcm[:16000*2*10]
	}
	extractor := fingerprint.New()
	extractor.MaxPrints = c.MaxPrints
	prints, e := extractor.Extract(ctx, pcm)
	if e != nil {
		return e
	}
	pool, e := pgxpool.New(ctx, c.DatabaseURL)
	if e != nil {
		return e
	}
	defer pool.Close()
	repo := store.Store{Pool: pool, Window: c.SearchWindow, MaxHits: c.MaxHits, MaxPostings: c.MaxPostings, Match: c.Match}
	plan, e := repo.ExplainCandidates(ctx, *scope, prints)
	if e != nil {
		return e
	}
	for _, line := range plan {
		fmt.Println(line)
	}
	return nil
}
