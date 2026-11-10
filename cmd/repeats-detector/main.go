package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := execute(ctx, os.Args[0], os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func execute(ctx context.Context, name string, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	options, err := parseOptions(name, args, stderr)
	if err == flag.ErrHelp {
		return 0
	}
	if err != nil {
		return 2
	}
	if err := run(ctx, options, getenv("AUDIO_PRESET"), getenv("DATABASE_URL"), stdout, stderr); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func run(ctx context.Context, options options, preset, dsn string, stdout, stderr io.Writer) error {
	extractor, decode, err := selectPreset(preset)
	if err != nil {
		return err
	}
	files, err := listPCMFiles(options.directory)
	if err != nil {
		return err
	}
	repo, err := openStore(ctx, dsn, extractor.Version(), options.migrate)
	if err != nil {
		return err
	}
	defer repo.Pool.Close()

	processor := fileProcessor{repo: repo, extractor: extractor, decode: decode, scope: options.scope}
	return processBatch(ctx, files, processor, stdout, stderr)
}
