// evaluate compares a query WAV against a reference WAV without infrastructure.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"repeats-detector/internal/audio"
	"repeats-detector/internal/domain"
	"repeats-detector/internal/fingerprint"
	"repeats-detector/internal/matching"
)

type Case struct {
	Name                  string   `json:"name"`
	Reference             string   `json:"reference"`
	Query                 string   `json:"query"`
	Expected              bool     `json:"expected"`
	ExpectedOriginalStart *float64 `json:"expected_original_start_sec,omitempty"`
}
type Report struct {
	Name         string        `json:"name"`
	Expected     bool          `json:"expected"`
	Detected     bool          `json:"detected"`
	Milliseconds float64       `json:"milliseconds"`
	Prints       int           `json:"fingerprints"`
	Match        *domain.Match `json:"match,omitempty"`
	OffsetError  *float64      `json:"offset_error_sec,omitempty"`
}

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	manifest := flag.String("manifest", "", "JSON array of labeled cases; WAV paths relative to manifest")
	method := flag.String("method", "landmarks", "landmarks, landmarks-coarse or exact-pcm")
	cfg := matching.Default()
	flag.Float64Var(&cfg.MinDuration, "min-duration", cfg.MinDuration, "minimum matched seconds")
	flag.IntVar(&cfg.MinFrames, "min-frames", cfg.MinFrames, "minimum independent frames")
	flag.Float64Var(&cfg.MinCoverage, "min-coverage", cfg.MinCoverage, "minimum frame coverage")
	flag.Float64Var(&cfg.MaxGap, "max-gap", cfg.MaxGap, "maximum gap seconds")
	flag.Parse()
	if cfg.MinDuration <= 0 || cfg.MinFrames < 1 || cfg.MinCoverage <= 0 || cfg.MinCoverage > 1 || cfg.MaxGap <= 0 {
		return fmt.Errorf("invalid matching thresholds")
	}
	if *manifest == "" {
		return fmt.Errorf("-manifest is required")
	}
	if *method != "landmarks" && *method != "landmarks-coarse" && *method != "exact-pcm" {
		return fmt.Errorf("unknown method")
	}
	raw, e := os.ReadFile(*manifest)
	if e != nil {
		return e
	}
	var cases []Case
	if e = json.Unmarshal(raw, &cases); e != nil {
		return e
	}
	if len(cases) == 0 {
		return fmt.Errorf("empty manifest")
	}
	reports := []Report{}
	tp, fp, fn, tn := 0, 0, 0, 0
	for _, c := range cases {
		start := time.Now()
		ref, e := read(filepath.Dir(*manifest), c.Reference)
		if e != nil {
			return e
		}
		query, e := read(filepath.Dir(*manifest), c.Query)
		if e != nil {
			return e
		}
		r := Report{Name: c.Name, Expected: c.Expected}
		if *method == "exact-pcm" {
			r.Detected = sha256.Sum256(ref) == sha256.Sum256(query)
		} else {
			ex := fingerprint.New()
			if *method == "landmarks-coarse" {
				ex.BinWidth = 4
			}
			rp, e := ex.Extract(context.Background(), ref)
			if e != nil {
				return e
			}
			qp, e := ex.Extract(context.Background(), query)
			if e != nil {
				return e
			}
			r.Prints = len(rp) + len(qp)
			index := map[int64][]int{}
			for _, p := range rp {
				index[p.Hash] = append(index[p.Hash], p.Frame)
			}
			hits := []domain.Hit{}
			for _, p := range qp {
				for _, f := range index[p.Hash] {
					hits = append(hits, domain.Hit{Hash: p.Hash, QueryFrame: p.Frame, OriginalFrame: f, TrackID: "reference"})
					if len(hits) > 1000000 {
						return fmt.Errorf("%s: hit limit exceeded", c.Name)
					}
				}
			}
			r.Match = matching.Best(hits, cfg)
			r.Detected = r.Match != nil
			if r.Match != nil && c.ExpectedOriginalStart != nil {
				v := r.Match.OriginalStartSec - r.Match.QueryStartSec - *c.ExpectedOriginalStart
				if v < 0 {
					v = -v
				}
				r.OffsetError = &v
			}
		}
		r.Milliseconds = float64(time.Since(start).Microseconds()) / 1000
		reports = append(reports, r)
		if r.Expected {
			if r.Detected {
				tp++
			} else {
				fn++
			}
		} else {
			if r.Detected {
				fp++
			} else {
				tn++
			}
		}
	}
	precision, recall := 0.0, 0.0
	if tp+fp > 0 {
		precision = float64(tp) / float64(tp+fp)
	}
	if tp+fn > 0 {
		recall = float64(tp) / float64(tp+fn)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{"method": *method, "algorithm_version": fingerprint.Version, "config": cfg, "cases": reports, "tp": tp, "fp": fp, "fn": fn, "tn": tn, "precision": precision, "recall": recall, "experimental": true})
}
func read(base, path string) (audio.PCM, error) {
	if !filepath.IsAbs(path) {
		path = filepath.Join(base, path)
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return nil, e
	}
	if info.Size() > 256000000 {
		return nil, fmt.Errorf("WAV too large")
	}
	b, e := os.ReadFile(path)
	if e != nil {
		return nil, e
	}
	return audio.Parse(b)
}
