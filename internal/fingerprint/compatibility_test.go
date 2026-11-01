package fingerprint_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"repeats-detector/internal/audio"
	"repeats-detector/internal/fingerprint"
	"repeats-detector/internal/testaudio"
)

// These digests pin the ordered (hash, frame) output of the pre-refactor v2
// extractor. Encoding is explicit to avoid depending on JSON or Go formatting.
func TestV2FingerprintCompatibility(t *testing.T) {
	ref := testaudio.Signal(42, 10)
	for _, tc := range []struct {
		name     string
		pcm      audio.PCM
		binWidth int
		want     string
	}{
		{"signal", ref, 2, "a6e85efd5a2267350e6ca78d974b7152f4e8d0c5c09c41c68ee4b217282dd8b7"},
		{"other-signal", testaudio.Signal(999, 6), 2, "f13e6df5a0102f36ec2a1d7654f356209ba32ce1ee9dfba09e0bec4df2123cb4"},
		{"noisy-crop", testaudio.Noise(testaudio.Crop(ref, 1.357, 6), 8), 2, "823894a7a42bc2b3a1935123839dd1787423c11dc08fce99a1c61677f13426f5"},
		{"coarse", ref, 4, "37a446ecf5feff1b2b303b6aaa300bbecfa650604cb91d3060e287d121a43688"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex := fingerprint.New()
			ex.BinWidth = tc.binWidth
			prints, err := ex.Extract(context.Background(), tc.pcm)
			if err != nil {
				t.Fatal(err)
			}
			h := sha256.New()
			var buf [16]byte
			for _, p := range prints {
				binary.LittleEndian.PutUint64(buf[:8], uint64(p.Hash))
				binary.LittleEndian.PutUint64(buf[8:], uint64(p.Frame))
				_, _ = h.Write(buf[:])
			}
			got := fmt.Sprintf("%x", h.Sum(nil))
			if got != tc.want {
				t.Fatalf("%d prints: digest=%s, want %s", len(prints), got, tc.want)
			}
		})
	}
}
