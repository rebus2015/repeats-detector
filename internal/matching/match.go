package matching

import (
	"context"
	"math"
	"sort"
	"time"

	"repeats-detector/internal/domain"
	"repeats-detector/internal/fingerprint"
)

type Config struct {
	MinDuration float64
	MinFrames   int
	MinCoverage float64
	MaxGap      float64
	Tolerance   int
}

func Default() Config {
	return Config{MinDuration: 2, MinFrames: 12, MinCoverage: 0.08, MaxGap: 0.8, Tolerance: 2}
}

type frameEvidence struct {
	hash      int64
	confirmed bool
}

type alignment struct {
	track string
	delta int
}

// Best requires independent query frames and temporal coverage, not raw hash votes.
func Best(hits []domain.Hit, c Config) *domain.Match {
	m, _ := BestContext(context.Background(), hits, c)
	return m
}
func BestContext(ctx context.Context, hits []domain.Hit, c Config) (*domain.Match, error) {
	groups := make(map[alignment]map[int]frameEvidence)
	ages := make(map[string]time.Time)
	for i, h := range hits {
		if i%1024 == 0 {
			if e := ctx.Err(); e != nil {
				return nil, e
			}
		}
		if age, ok := ages[h.TrackID]; !ok || h.CreatedAt.Before(age) {
			ages[h.TrackID] = h.CreatedAt
		}
		delta := h.OriginalFrame - h.QueryFrame
		for d := delta - c.Tolerance; d <= delta+c.Tolerance; d++ {
			key := alignment{h.TrackID, d}
			if groups[key] == nil {
				groups[key] = make(map[int]frameEvidence)
			}
			e, exists := groups[key][h.QueryFrame]
			if !exists {
				e.hash = h.Hash
			} else if e.hash != h.Hash {
				e.confirmed = true
			}
			groups[key][h.QueryFrame] = e
		}
	}
	var best *domain.Match
	var bestAge time.Time
	for key, set := range groups {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		if len(set) < c.MinFrames {
			continue
		}
		frames := make([]int, 0, len(set))
		for f, e := range set {
			if e.confirmed {
				frames = append(frames, f)
			}
		}
		if len(frames) < c.MinFrames {
			continue
		}
		sort.Ints(frames)
		start := 0
		for end := 0; end < len(frames); end++ {
			if end+1 < len(frames) && float64(frames[end+1]-frames[end])*fingerprint.FrameSeconds <= c.MaxGap {
				continue
			}
			n := end - start + 1
			span := frames[end] - frames[start] + 1
			duration := float64(span) * fingerprint.FrameSeconds
			coverage := float64(n) / float64(span)
			original := frames[start] + key.delta
			if n >= c.MinFrames && duration >= c.MinDuration && coverage >= c.MinCoverage && original >= 0 {
				m := &domain.Match{OriginalTrackID: key.track, QueryStartSec: float64(frames[start]) * fingerprint.FrameSeconds, OriginalStartSec: float64(original) * fingerprint.FrameSeconds, MatchedDurationSec: duration, Score: math.Min(1, coverage), Evidence: n}
				age := ages[m.OriginalTrackID]
				if best == nil || m.Evidence > best.Evidence || (m.Evidence == best.Evidence && (age.Before(bestAge) || (age.Equal(bestAge) && (m.OriginalTrackID < best.OriginalTrackID || (m.OriginalTrackID == best.OriginalTrackID && m.OriginalStartSec < best.OriginalStartSec))))) {
					best = m
					bestAge = age
				}
			}
			start = end + 1
		}
	}
	return best, nil
}
