package fingerprint

import (
	"context"
	"errors"
	"math"
	"testing"

	"repeats-detector/internal/audio"
	"repeats-detector/internal/domain"
	"repeats-detector/internal/testaudio"
)

func TestLocalPeakNeighborhoodAndPlateaus(t *testing.T) {
	const center, bin = 10, 20 // center crosses the ring boundary
	for _, tc := range []struct {
		name   string
		dt, df int
		power  float64
		want   bool
	}{
		{"isolated", 1, 0, 0, true},
		{"earlier-equal", -1, 0, 10, false},
		{"later-equal", 1, 0, 10, true},
		{"lower-bin-equal", 0, -1, 10, false},
		{"higher-bin-equal", 0, 1, 10, true},
		{"past-corner-stronger", -3, -3, 11, false},
		{"future-corner-stronger", 3, 3, 11, false},
		{"outside-frequency-radius", 0, 4, 11, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ring spectrumRing
			ring[center%len(ring)][bin] = 10
			ring[(center+tc.dt)%len(ring)][bin+tc.df] = tc.power
			if got := ring.isLocalPeak(center, bin); got != tc.want {
				t.Fatalf("local=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestFramePeakThresholdsAndRanking(t *testing.T) {
	var ring spectrumRing
	row := &ring[3]
	// All candidates are sufficiently far apart to avoid suppressing each other.
	row[10], row[20], row[30], row[40], row[50] = 5, 100, 20, 10, 4
	got := ring.framePeaks(3)
	if len(got) != 3 || got[0].bin != 20 || got[1].bin != 30 || got[2].bin != 40 {
		t.Fatalf("wrong strongest peaks: %+v", got)
	}
	row[10], row[30], row[40] = 0, 0, 0
	got = ring.framePeaks(3)
	if len(got) != 1 || got[0].bin != 20 {
		t.Fatalf("relative threshold must exclude 4%%: %+v", got)
	}
	clear(row[:])
	row[10] = 0.5e-8
	if got := ring.framePeaks(3); len(got) != 0 {
		t.Fatalf("weak peak accepted: %+v", got)
	}
	row[10] = 1e-8
	if got := ring.framePeaks(3); len(got) != 1 {
		t.Fatalf("peak at absolute threshold lost: %+v", got)
	}
}

func TestSpectrumClearsReusedSlot(t *testing.T) {
	ex := New()
	var scratch [Size]complex128
	var row powerSpectrum
	ex.computeSpectrum(testaudio.Signal(42, 1), 0, &scratch, &row)
	nonzero := false
	for _, v := range row {
		nonzero = nonzero || v > 0
	}
	if !nonzero {
		t.Fatal("non-silent spectrum is empty")
	}
	ex.computeSpectrum(make(audio.PCM, Size*2), 0, &scratch, &row)
	for bin, v := range row {
		if v != 0 {
			t.Fatalf("stale power at bin %d: %g", bin, v)
		}
	}
}

func TestFFTConstantSignal(t *testing.T) {
	var data [Size]complex128
	for i := range data {
		data[i] = 1
	}
	fft(data[:])
	if real(data[0]) != Size || imag(data[0]) != 0 {
		t.Fatalf("wrong DC: %v", data[0])
	}
	for bin, v := range data[1:] {
		if math.Hypot(real(v), imag(v)) > 1e-10 {
			t.Fatalf("unexpected non-DC power at %d: %v", bin+1, v)
		}
	}
}

func TestPairingDistanceAndHash(t *testing.T) {
	ex := New()
	prints, err := ex.pairPeaks(context.Background(), []peak{
		{bin: 20, frame: 0}, {bin: 30, frame: 3}, {bin: 40, frame: 4},
		{bin: 50, frame: 64}, {bin: 60, frame: 65},
	})
	if err != nil {
		t.Fatal(err)
	}
	var anchor []domain.Print
	for _, p := range prints {
		if p.Frame == 0 {
			anchor = append(anchor, p)
		}
	}
	if len(anchor) != 2 || anchor[0].Hash != int64(10<<20|20<<10|2) || anchor[1].Hash != int64(10<<20|25<<10|32) {
		t.Fatalf("wrong distance limits or hash encoding: %+v", anchor)
	}
}

func TestPairingTargetCountLimitAndCancellation(t *testing.T) {
	ex := New()
	peaks := []peak{{bin: 20, frame: 0}}
	for i := 0; i < 7; i++ {
		peaks = append(peaks, peak{bin: 40 + i*2, frame: 4 + i})
	}
	prints, err := ex.pairPeaks(context.Background(), peaks)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, p := range prints {
		if p.Frame == 0 {
			count++
			if p.Hash == int64(10<<20|26<<10|5) {
				t.Fatal("seventh target included")
			}
		}
	}
	if count != 6 {
		t.Fatalf("anchor produced %d pairs, want 6", count)
	}
	ex.MaxPrints = 1
	if p, err := ex.pairPeaks(context.Background(), peaks); !errors.Is(err, domain.ErrInvalid) || p != nil {
		t.Fatalf("limit must discard partial output: %+v %v", p, err)
	}
	if p, err := ex.pairPeaks(context.Background(), peaks[:2]); err != nil || len(p) != 1 {
		t.Fatalf("exact limit rejected: %+v %v", p, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if p, err := ex.pairPeaks(ctx, peaks); !errors.Is(err, context.Canceled) || p != nil {
		t.Fatalf("cancellation must discard output: %+v %v", p, err)
	}
}

func TestExtractPeakTemporalBounds(t *testing.T) {
	pcm := testaudio.Signal(42, 1)
	peaks, err := New().extractPeaks(context.Background(), pcm)
	if err != nil {
		t.Fatal(err)
	}
	if len(peaks) == 0 {
		t.Fatal("signal must have peaks")
	}
	last := (pcm.Len()-Size)/Hop - radius
	for _, p := range peaks {
		if p.frame < radius || p.frame > last {
			t.Fatalf("peak without full temporal support: %+v, last=%d", p, last)
		}
	}
}

func TestExtractValidationAndLimit(t *testing.T) {
	pcm := testaudio.Signal(42, 1)
	for _, tc := range []struct {
		name                string
		binWidth, maxPrints int
	}{
		{"zero-bin-width", 0, 100},
		{"too-large-bin-width", 17, 100},
		{"zero-limit", 2, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex := New()
			ex.BinWidth, ex.MaxPrints = tc.binWidth, tc.maxPrints
			if prints, err := ex.Extract(context.Background(), pcm); err == nil || prints != nil {
				t.Fatalf("invalid config accepted: %+v %v", prints, err)
			}
		})
	}
	ex := New()
	ex.MaxPrints = 1
	if prints, err := ex.Extract(context.Background(), pcm); !errors.Is(err, domain.ErrInvalid) || prints != nil {
		t.Fatalf("Extract lost the pairing limit: %+v %v", prints, err)
	}
	if prints, err := ex.Extract(context.Background(), make(audio.PCM, 2*(Size-1))); err != nil || len(prints) != 0 {
		t.Fatalf("short input must be empty: %+v %v", prints, err)
	}
}
