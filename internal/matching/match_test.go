package matching

import (
	"math/rand"
	"repeats-detector/internal/fingerprint"
	"strconv"
	"testing"

	"repeats-detector/internal/domain"
)

func TestTemporalEvidence(t *testing.T) {
	var h []domain.Hit
	for i := 0; i < 200; i++ {
		h = append(h, domain.Hit{Hash: 1, QueryFrame: i, OriginalFrame: i + 100, TrackID: "a"}, domain.Hit{Hash: 2, QueryFrame: i, OriginalFrame: i + 100, TrackID: "a"})
	}
	if m := Best(h, Default()); m == nil || m.OriginalTrackID != "a" {
		t.Fatal(m)
	}
	for i := range h {
		h[i].OriginalFrame = 500 - i
	}
	if m := Best(h, Default()); m != nil {
		t.Fatal("reverse sequence matched", m)
	}
	for i := range h {
		h[i] = domain.Hit{QueryFrame: 1, OriginalFrame: 101, TrackID: "a"}
	}
	if m := Best(h, Default()); m != nil {
		t.Fatal("one frame counted repeatedly", m)
	}
}

func TestMatchSurvivesSparseSurroundingEvidence(t *testing.T) {
	for _, shift := range []int{0, 450} {
		t.Run(strconv.Itoa(shift), func(t *testing.T) {
			var hits []domain.Hit
			add := func(f int) {
				for _, hash := range []int64{1, 2} {
					hits = append(hits, domain.Hit{Hash: hash, QueryFrame: f, OriginalFrame: f + 100, TrackID: "original"})
				}
			}
			for f := shift; f <= shift+132; f += 12 {
				add(f)
			}
			if Best(hits, Default()) == nil {
				t.Fatal("baseline must match")
			}
			if shift == 0 {
				for f := 182; f <= 582; f += 50 {
					add(f)
				}
			} else {
				for f := 0; f <= 400; f += 50 {
					add(f)
				}
			}
			m := Best(hits, Default())
			if m == nil || m.Evidence < 12 {
				t.Fatalf("sparse evidence hid the valid interval: %+v", m)
			}
		})
	}
}

// Compare the interval search with exhaustive enumeration, including chains
// with sparse prefixes, tails and gaps. Evidence is the ranking criterion.
func TestIntervalSearchAgainstExhaustive(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for trial := 0; trial < 200; trial++ {
		c := Default()
		c.MinCoverage = []float64{0.08, 0.2, 0.5, 1}[trial%4]
		var frames []int
		var hits []domain.Hit
		f := 0
		for i := 0; i < 100; i++ {
			f += 1 + rng.Intn(60)
			frames = append(frames, f)
			for _, hash := range []int64{1, 2} {
				hits = append(hits, domain.Hit{Hash: hash, QueryFrame: f, OriginalFrame: f + 100, TrackID: "original"})
			}
		}
		want := 0
		for start := range frames {
			for end := start; end < len(frames); end++ {
				if end > start && float64(frames[end]-frames[end-1])*fingerprint.FrameSeconds > c.MaxGap {
					break
				}
				n, span := end-start+1, frames[end]-frames[start]+1
				if n >= c.MinFrames && float64(span)*fingerprint.FrameSeconds >= c.MinDuration && float64(n)/float64(span) >= c.MinCoverage && n > want {
					want = n
				}
			}
		}
		got := 0
		if m := Best(hits, c); m != nil {
			got = m.Evidence
		}
		if got != want {
			t.Fatalf("trial %d: evidence=%d, want %d", trial, got, want)
		}
	}
}
