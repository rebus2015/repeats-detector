package fingerprint_test

import (
	"context"
	"math"
	"testing"

	"repeats-detector/internal/audio"
	"repeats-detector/internal/domain"
	"repeats-detector/internal/fingerprint"
	"repeats-detector/internal/matching"
	"repeats-detector/internal/testaudio"
)

func TestTransformedFragment(t *testing.T) {
	ref := testaudio.Signal(42, 10)
	ex := fingerprint.New()
	rp, e := ex.Extract(context.Background(), ref)
	if e != nil {
		t.Fatal(e)
	}
	for _, noise := range []float64{100, 18, 8} {
		t.Run(string(rune(int(noise)+65)), func(t *testing.T) {
			q := testaudio.Noise(testaudio.Crop(ref, 1.357, 6), noise)
			qp, e := ex.Extract(context.Background(), q)
			if e != nil {
				t.Fatal(e)
			}
			hits := hits(rp, qp)
			m := matching.Best(hits, matching.Default())
			if m == nil {
				t.Fatal("fragment not detected", noise, len(hits))
			}
			if math.Abs(m.OriginalStartSec-m.QueryStartSec-1.357) > 0.08 {
				t.Fatal("wrong alignment", m)
			}
		})
	}
	qp, _ := ex.Extract(context.Background(), testaudio.Signal(999, 6))
	if m := matching.Best(hits(rp, qp), matching.Default()); m != nil {
		t.Fatalf("unrelated signal matched: %+v", m)
	}
}
func hits(r, q []domain.Print) []domain.Hit {
	idx := map[int64][]int{}
	for _, p := range r {
		idx[p.Hash] = append(idx[p.Hash], p.Frame)
	}
	out := []domain.Hit{}
	for _, p := range q {
		for _, f := range idx[p.Hash] {
			out = append(out, domain.Hit{Hash: p.Hash, QueryFrame: p.Frame, OriginalFrame: f, TrackID: "ref"})
		}
	}
	return out
}
func TestSilenceAndCancellation(t *testing.T) {
	ex := fingerprint.New()
	p, e := ex.Extract(context.Background(), make(audio.PCM, 32000))
	if e != nil || len(p) != 0 {
		t.Fatal("silence must not yield evidence")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = ex.Extract(ctx, testaudio.Signal(1, 1)); e == nil {
		t.Fatal("cancellation ignored")
	}
}
func BenchmarkExtract(b *testing.B) {
	p := testaudio.Signal(2, 10)
	ex := fingerprint.New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ex.Extract(context.Background(), p)
	}
}
