package matching

import (
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
