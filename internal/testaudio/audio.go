// Package testaudio provides deterministic signals for mechanical tests only.
// They are not a representative speech corpus.
package testaudio

import (
	"encoding/binary"
	"math"
	"math/rand"

	"repeats-detector/internal/audio"
)

func Signal(seed int64, seconds float64) audio.PCM {
	r := rand.New(rand.NewSource(seed))
	n := int(seconds * audio.SampleRate)
	p := make(audio.PCM, n*2)
	freqs := make([][4]float64, int(seconds*8)+2)
	for i := range freqs {
		for j := range freqs[i] {
			freqs[i][j] = 150 + r.Float64()*5000
		}
	}
	phases := [4]float64{}
	for i := 0; i < n; i++ {
		v := 0.0
		epoch := i / (audio.SampleRate / 8)
		for j := range phases {
			phases[j] += 2 * math.Pi * freqs[epoch][j] / audio.SampleRate
			v += 0.12 * math.Sin(phases[j])
		}
		binary.LittleEndian.PutUint16(p[2*i:], uint16(int16(v*32767)))
	}
	return p
}
func Crop(p audio.PCM, start, seconds float64) audio.PCM {
	lo := int(start*audio.SampleRate) * 2
	hi := lo + int(seconds*audio.SampleRate)*2
	return append(audio.PCM{}, p[lo:hi]...)
}
func Noise(p audio.PCM, snr float64) audio.PCM {
	energy := 0.0
	for i := 0; i < p.Len(); i++ {
		v := p.Sample(i)
		energy += v * v
	}
	amplitude := math.Sqrt(energy/float64(p.Len())) / math.Pow(10, snr/20)
	r := rand.New(rand.NewSource(91))
	out := make(audio.PCM, len(p))
	for i := 0; i < p.Len(); i++ {
		v := p.Sample(i) + r.NormFloat64()*amplitude
		v = math.Max(-1, math.Min(1, v))
		binary.LittleEndian.PutUint16(out[2*i:], uint16(int16(v*32767)))
	}
	return out
}
func WAV(p audio.PCM) []byte {
	b := make([]byte, 44+len(p))
	copy(b, "RIFF")
	binary.LittleEndian.PutUint32(b[4:], uint32(len(b)-8))
	copy(b[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(b[16:], 16)
	binary.LittleEndian.PutUint16(b[20:], 1)
	binary.LittleEndian.PutUint16(b[22:], 1)
	binary.LittleEndian.PutUint32(b[24:], 16000)
	binary.LittleEndian.PutUint32(b[28:], 32000)
	binary.LittleEndian.PutUint16(b[32:], 2)
	binary.LittleEndian.PutUint16(b[34:], 16)
	copy(b[36:], "data")
	binary.LittleEndian.PutUint32(b[40:], uint32(len(p)))
	copy(b[44:], p)
	return b
}
