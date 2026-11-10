package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"repeats-detector/internal/audio"
	"repeats-detector/internal/domain"
	"repeats-detector/internal/fingerprint"
	"repeats-detector/internal/matching"
	"repeats-detector/internal/store"
)

// maxAudioBytes ограничивает объём данных, читаемых перед декодированием.
const maxAudioBytes = 128_000_000

func main() {
	scope := flag.String("scope", "local", "группа записей для поиска повторов")
	migrate := flag.Bool("migrate", true, "создать таблицы PostgreSQL при запуске")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Использование: %s [параметры] <папка с аудио>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 || strings.TrimSpace(*scope) == "" || len(*scope) > 256 {
		flag.Usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, flag.Arg(0), *scope, *migrate); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, directory, scope string, migrate bool) error {
	directory, err := filepath.Abs(directory)
	if err != nil {
		return err
	}
	// ReadDir сортирует записи по имени, задавая порядок добавления файлов в индекс.
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	var files []string
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		if strings.EqualFold(filepath.Ext(entry.Name()), ".pcm") {
			files = append(files, filepath.Join(directory, entry.Name()))
		}
	}
	if len(files) == 0 {
		return fmt.Errorf("в папке %q нет файлов .pcm", directory)
	}

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("задайте DATABASE_URL для подключения к PostgreSQL")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return errors.New("некорректный DATABASE_URL")
	}
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(startup, config)
	if err != nil {
		return err
	}
	defer pool.Close()
	repo := &store.Store{Pool: pool, MaxHits: 200_000, MaxPostings: 32, Match: matching.Default()}
	if migrate {
		err = repo.Migrate(startup)
	} else {
		err = repo.CheckSchema(startup)
	}
	cancel()
	if err != nil {
		return fmt.Errorf("подготовка PostgreSQL: %w", err)
	}

	extractor := fingerprint.New()
	output := json.NewEncoder(os.Stdout)
	failed := 0
	for _, path := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		result, err := processFile(ctx, repo, extractor, path, scope)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			fmt.Fprintf(os.Stderr, "%q: %v\n", path, err)
			failed++
			continue
		}
		// Decide возвращает результат после фиксации транзакции или чтения готовой записи.
		if err := output.Encode(result); err != nil {
			return fmt.Errorf("вывод результата для %q: %w", path, err)
		}
	}
	if failed > 0 {
		return fmt.Errorf("файлов с ошибками: %d из %d", failed, len(files))
	}
	return nil
}

func processFile(ctx context.Context, repo *store.Store, extractor *fingerprint.Extractor, path, scope string) (domain.Result, error) {
	file, err := os.Open(path)
	if err != nil {
		return domain.Result{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxAudioBytes+1))
	if err != nil {
		return domain.Result{}, err
	}
	if len(data) > maxAudioBytes {
		return domain.Result{}, fmt.Errorf("размер файла превышает %d байт", maxAudioBytes)
	}

	pcm, err := audio.DecodeALaw(data)
	if err != nil {
		return domain.Result{}, err
	}
	// Версия алгоритма, путь и содержимое определяют идентификатор задания.
	// При повторном запуске тот же файл получает тот же идентификатор.
	hash := sha256.New()
	fmt.Fprintf(hash, "%s\x00%s\x00", fingerprint.Version, path)
	hash.Write(data)
	task := domain.Task{TaskID: hex.EncodeToString(hash.Sum(nil)), File: path, ScopeID: scope}

	prints, err := extractor.Extract(ctx, pcm)
	if err != nil {
		return domain.Result{}, err
	}
	return repo.Decide(ctx, task, prints)
}
