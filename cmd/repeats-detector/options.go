package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"repeats-detector/internal/audio"
	"repeats-detector/internal/fingerprint"
)

type options struct {
	directory string
	scope     string
	migrate   bool
}

func parseOptions(name string, args []string, stderr io.Writer) (options, error) {
	var result options
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&result.scope, "scope", "local", "группа записей для поиска повторов")
	flags.BoolVar(&result.migrate, "migrate", true, "создать таблицы PostgreSQL при запуске")
	flags.Usage = func() {
		fmt.Fprintf(stderr, "Использование: %s [параметры] <папка с аудио>\n", name)
		fmt.Fprintln(stderr, "AUDIO_PRESET: 8khz-alaw (по умолчанию) или 16khz-s16le")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() != 1 || strings.TrimSpace(result.scope) == "" || len(result.scope) > 256 {
		flags.Usage()
		return options{}, errors.New("некорректные аргументы")
	}
	result.directory = flags.Arg(0)
	return result, nil
}

type audioDecoder func([]byte) (audio.PCM, error)

func selectPreset(preset string) (*fingerprint.Extractor, audioDecoder, error) {
	switch preset {
	case "", "8khz-alaw":
		return fingerprint.New8kHz(), audio.DecodeALaw, nil
	case "16khz-s16le":
		return fingerprint.New16kHz(), audio.DecodeS16LE, nil
	default:
		return nil, nil, fmt.Errorf("некорректный AUDIO_PRESET %q, доступны 8khz-alaw и 16khz-s16le", preset)
	}
}
