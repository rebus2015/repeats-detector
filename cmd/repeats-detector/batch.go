package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"repeats-detector/internal/domain"
	"repeats-detector/internal/fingerprint"
)

// maxAudioBytes ограничивает объём данных, читаемых перед декодированием.
const maxAudioBytes = 128_000_000

type resultStore interface {
	Decide(context.Context, domain.Task, []domain.Print) (domain.Result, error)
}

type fileProcessor struct {
	repo      resultStore
	extractor *fingerprint.Extractor
	decode    audioDecoder
	scope     string
}

func listPCMFiles(directory string) ([]string, error) {
	directory, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	// ReadDir сортирует записи по имени, задавая порядок добавления файлов в индекс.
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("в папке %q нет файлов .pcm", directory)
	}
	return files, nil
}

func processBatch(ctx context.Context, files []string, processor fileProcessor, stdout, stderr io.Writer) error {
	output := json.NewEncoder(stdout)
	failed := 0
	for _, path := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		result, err := processor.processFile(ctx, path)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			fmt.Fprintf(stderr, "%q: %v\n", path, err)
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

func (p fileProcessor) processFile(ctx context.Context, path string) (domain.Result, error) {
	data, err := readAudioFile(path)
	if err != nil {
		return domain.Result{}, err
	}
	pcm, err := p.decode(data)
	if err != nil {
		return domain.Result{}, err
	}
	// Версия алгоритма, путь и содержимое определяют идентификатор задания.
	// При повторном запуске тот же файл получает тот же идентификатор.
	hash := sha256.New()
	fmt.Fprintf(hash, "%s\x00%s\x00", p.extractor.Version(), path)
	hash.Write(data)
	task := domain.Task{TaskID: hex.EncodeToString(hash.Sum(nil)), File: path, ScopeID: p.scope}

	prints, err := p.extractor.Extract(ctx, pcm)
	if err != nil {
		return domain.Result{}, err
	}
	return p.repo.Decide(ctx, task, prints)
}

func readAudioFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxAudioBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxAudioBytes {
		return nil, fmt.Errorf("размер файла превышает %d байт", maxAudioBytes)
	}
	return data, nil
}
