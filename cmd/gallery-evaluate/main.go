// gallery-evaluate measures retrieval against a whole gallery, including wrong-track
// retrievals, candidate overflow and timing. It does not exercise S3, SQL or AMQP.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"repeats-detector/internal/audio"
	"repeats-detector/internal/domain"
	"repeats-detector/internal/fingerprint"
	"repeats-detector/internal/matching"
)

type recording struct {
	TrackID string `json:"track_id"`
	Path    string `json:"path"`
}
type query struct {
	Name           string  `json:"name"`
	Path           string  `json:"path"`
	ExpectedTrack  string  `json:"expected_track"`
	ExpectedOffset float64 `json:"expected_offset_sec"`
	Seconds        int     `json:"seconds"`
	Transform      string  `json:"transform"`
}
type manifest struct {
	Gallery        []recording `json:"gallery"`
	Queries        []query     `json:"queries"`
	GallerySeconds float64     `json:"gallery_seconds"`
}
type posting struct {
	track int
	frame int
}
type result struct {
	query
	Match        *domain.Match `json:"match,omitempty"`
	Correct      bool          `json:"correct"`
	Hits         int           `json:"hits"`
	Error        string        `json:"error,omitempty"`
	Milliseconds float64       `json:"milliseconds"`
	OffsetError  *float64      `json:"offset_error_sec,omitempty"`
}

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	path := flag.String("manifest", "", "manifest from prepare_long_corpus.py")
	output := flag.String("output", "", "report path (stdout if empty)")
	maxHits := flag.Int("max-hits", 200000, "fail query explicitly above this bound")
	maxPostings := flag.Int("max-postings", 32, "ignore hash occurring more often in the gallery; 0 disables")
	limit := flag.Int("limit", 0, "query limit for diagnostics; 0 means all")
	flag.Parse()
	if *path == "" || *maxHits < 1 || *limit < 0 {
		return fmt.Errorf("invalid arguments")
	}
	raw, e := os.ReadFile(*path)
	if e != nil {
		return e
	}
	var m manifest
	if e = json.Unmarshal(raw, &m); e != nil {
		return e
	}
	if len(m.Gallery) == 0 || len(m.Queries) == 0 {
		return fmt.Errorf("empty gallery or queries")
	}
	base := filepath.Dir(*path)
	ex := fingerprint.New()
	index := map[int64][]posting{}
	buildStart := time.Now()
	prints := 0
	for i, r := range m.Gallery {
		pcm, e := read(base, r.Path)
		if e != nil {
			return e
		}
		ps, e := ex.Extract(context.Background(), pcm)
		if e != nil {
			return e
		}
		for _, p := range ps {
			index[p.Hash] = append(index[p.Hash], posting{i, p.Frame})
		}
		prints += len(ps)
		fmt.Fprintf(os.Stderr, "indexed %d/%d: %s (%d prints)\n", i+1, len(m.Gallery), r.TrackID, len(ps))
	}
	fullIndex := index
	filteredHashes := map[int64]bool{}
	if *maxPostings > 0 {
		index = make(map[int64][]posting, len(fullIndex))
		for h, postings := range fullIndex {
			if len(postings) > *maxPostings {
				filteredHashes[h] = true
			} else {
				index[h] = postings
			}
		}
	}
	buildSeconds := time.Since(buildStart).Seconds()
	if *limit > 0 && *limit < len(m.Queries) {
		m.Queries = m.Queries[:*limit]
	}
	reports := make([]result, 0, len(m.Queries))
	timings := []float64{}
	tp, fp, fn, tn, wrong, overflow := 0, 0, 0, 0, 0, 0
	for i, q := range m.Queries {
		started := time.Now()
		r := result{query: q}
		pcm, e := read(base, q.Path)
		if e != nil {
			return e
		}
		ps, e := ex.Extract(context.Background(), pcm)
		if e != nil {
			return e
		}
		sort.Slice(ps, func(i, j int) bool { return ps[i].Frame < ps[j].Frame })
		width, overlap, minWidth := 625, 175, 300 // Same default window policy as Store.Decide.
		for start := 0; start < len(ps); {
			end := sort.Search(len(ps), func(i int) bool { return ps[i].Frame >= ps[start].Frame+width })
			batch := ps[start:end]
			seen, excluded := map[int64]bool{}, 0
			for _, p := range batch {
				if !seen[p.Hash] {
					seen[p.Hash] = true
					if filteredHashes[p.Hash] {
						excluded++
					}
				}
			}
			chosen := index
			if excluded*2 > len(seen) {
				chosen = fullIndex
			}
			hits := []domain.Hit{}
			tooMany := false
		search:
			for _, p := range batch {
				for _, v := range chosen[p.Hash] {
					if len(hits) >= *maxHits {
						tooMany = true
						break search
					}
					hits = append(hits, domain.Hit{Hash: p.Hash, QueryFrame: p.Frame, OriginalFrame: v.frame, TrackID: m.Gallery[v.track].TrackID})
				}
			}
			if tooMany {
				if width > minWidth {
					width = max(minWidth, width/2)
					continue
				}
				r.Error = "candidate limit exceeded"
				overflow++
				break
			}
			r.Hits += len(hits)
			candidate := matching.Best(hits, matching.Default())
			if candidate != nil && (r.Match == nil || candidate.Evidence > r.Match.Evidence) {
				r.Match = candidate
			}
			if end == len(ps) {
				break
			}
			next := sort.Search(len(ps), func(i int) bool { return ps[i].Frame >= ps[start].Frame+width-overlap })
			if next <= start {
				next = end
			}
			start = next
		}
		if r.Error == "" {
			if q.ExpectedTrack != "" {
				if r.Match != nil && r.Match.OriginalTrackID == q.ExpectedTrack {
					tp++
					r.Correct = true
					v := r.Match.OriginalStartSec - r.Match.QueryStartSec - q.ExpectedOffset
					if v < 0 {
						v = -v
					}
					r.OffsetError = &v
				} else {
					fn++
					if r.Match != nil {
						wrong++
						fp++
					}
				}
			} else if r.Match != nil {
				fp++
			} else {
				tn++
				r.Correct = true
			}
		} else if q.ExpectedTrack != "" {
			fn++
		}
		r.Milliseconds = float64(time.Since(started).Microseconds()) / 1000
		timings = append(timings, r.Milliseconds)
		reports = append(reports, r)
		if (i+1)%25 == 0 {
			fmt.Fprintf(os.Stderr, "queries %d/%d tp=%d fp=%d fn=%d tn=%d overflow=%d\n", i+1, len(m.Queries), tp, fp, fn, tn, overflow)
		}
	}
	sort.Float64s(timings)
	percentile := func(p float64) float64 {
		if len(timings) == 0 {
			return 0
		}
		return timings[int(float64(len(timings)-1)*p)]
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	report := map[string]any{"algorithm_version": fingerprint.Version, "config": matching.Default(), "gallery_seconds": m.GallerySeconds, "gallery_files": len(m.Gallery), "prints": prints, "index_build_seconds": buildSeconds, "tp": tp, "fp": fp, "fn": fn, "tn": tn, "wrong_track": wrong, "overflow": overflow, "queries": len(reports), "p50_ms": percentile(.5), "p95_ms": percentile(.95), "p99_ms": percentile(.99), "heap_sys_bytes": mem.HeapSys, "max_hits": *maxHits, "max_postings": *maxPostings, "filtered_hashes": len(filteredHashes), "cases": reports, "limitations": "In-memory retrieval; negative labels provisional; not an end-to-end service SLO."}
	out := os.Stdout
	if *output != "" {
		out, e = os.Create(*output)
		if e != nil {
			return e
		}
		defer out.Close()
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
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
		return nil, fmt.Errorf("WAV too large: %s", path)
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		return nil, e
	}
	return audio.Parse(raw)
}
