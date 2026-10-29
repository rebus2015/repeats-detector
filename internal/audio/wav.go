package audio

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"repeats-detector/internal/domain"
)

const SampleRate = 16000

// PCM points into the input buffer. No audio data is copied by Parse.
type PCM []byte

func (p PCM) Len() int { return len(p) / 2 }
func (p PCM) Sample(i int) float64 {
	return float64(int16(binary.LittleEndian.Uint16(p[2*i:]))) / 32768
}
func Parse(b []byte) (PCM, error) {
	bad := func(s string) (PCM, error) { return nil, fmt.Errorf("%w: WAV %s", domain.ErrInvalid, s) }
	if len(b) < 12 || string(b[:4]) != "RIFF" || string(b[8:12]) != "WAVE" {
		return bad("RIFF header")
	}
	end := uint64(binary.LittleEndian.Uint32(b[4:8])) + 8
	if end > uint64(len(b)) || end < 12 {
		return bad("truncated RIFF")
	}
	var data []byte
	valid := false
	for pos := uint64(12); pos+8 <= end; {
		n := uint64(binary.LittleEndian.Uint32(b[pos+4 : pos+8]))
		start := pos + 8
		if start+n > end {
			return bad("truncated chunk")
		}
		switch string(b[pos : pos+4]) {
		case "fmt ":
			if n < 16 {
				return bad("fmt length")
			}
			f := b[start : start+n]
			format := binary.LittleEndian.Uint16(f)
			if format == 0xfffe {
				pcmGUID := []byte{1, 0, 0, 0, 0, 0, 16, 0, 128, 0, 0, 170, 0, 56, 155, 113}
				if len(f) < 40 || binary.LittleEndian.Uint16(f[16:]) < 22 || binary.LittleEndian.Uint16(f[18:]) != 16 || !bytes.Equal(f[24:40], pcmGUID) {
					return bad("unsupported extensible format")
				}
				format = 1
			}
			if format != 1 || binary.LittleEndian.Uint16(f[2:]) != 1 || binary.LittleEndian.Uint32(f[4:]) != SampleRate || binary.LittleEndian.Uint32(f[8:]) != 32000 || binary.LittleEndian.Uint16(f[12:]) != 2 || binary.LittleEndian.Uint16(f[14:]) != 16 {
				return bad("requires PCM 16kHz mono 16bit")
			}
			valid = true
		case "data":
			if data != nil {
				return bad("multiple data chunks")
			}
			data = b[start : start+n]
		}
		pos = start + n + (n % 2)
	}
	if !valid || len(data) == 0 || len(data)%2 != 0 {
		return bad("missing fmt/data or incomplete sample")
	}
	return PCM(data), nil
}
