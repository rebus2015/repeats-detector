package audio_test

import (
	"encoding/binary"
	"testing"

	"repeats-detector/internal/audio"
	"repeats-detector/internal/testaudio"
)

func TestParse(t *testing.T) {
	b := testaudio.WAV(testaudio.Signal(1, 1))
	p, e := audio.Parse(b)
	if e != nil || p.Len() != 16000 {
		t.Fatal(e, p.Len())
	}
	p[0] = 99
	if b[44] != 99 {
		t.Fatal("PCM must share buffer")
	}
}
func TestExtraChunks(t *testing.T) {
	b := testaudio.WAV(testaudio.Signal(1, 1))
	x := append([]byte{}, b[:36]...)
	x = append(x, []byte{'J', 'U', 'N', 'K', 1, 0, 0, 0, 7, 0}...)
	x = append(x, b[36:]...)
	binary.LittleEndian.PutUint32(x[4:], uint32(len(x)-8))
	if _, e := audio.Parse(x); e != nil {
		t.Fatal(e)
	}
}
func TestInvalid(t *testing.T) {
	for _, kind := range []string{"truncated", "rate", "odd", "garbage"} {
		t.Run(kind, func(t *testing.T) {
			b := testaudio.WAV(testaudio.Signal(1, 1))
			switch kind {
			case "truncated":
				b = b[:len(b)-1]
			case "rate":
				binary.LittleEndian.PutUint32(b[24:], 8000)
			case "odd":
				binary.LittleEndian.PutUint32(b[40:], 31999)
			case "garbage":
				b = []byte("not wav")
			}
			if _, e := audio.Parse(b); e == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}
func FuzzParse(f *testing.F) {
	f.Add(testaudio.WAV(testaudio.Signal(1, 0.1)))
	f.Add([]byte("RIFF"))
	f.Fuzz(func(t *testing.T, b []byte) { _, _ = audio.Parse(b) })
}

func TestExtensiblePCM(t *testing.T) {
	b := testaudio.WAV(testaudio.Signal(1, 1))
	x := append([]byte{}, b[:36]...)
	binary.LittleEndian.PutUint32(x[16:], 40)
	binary.LittleEndian.PutUint16(x[20:], 0xfffe)
	extension := []byte{22, 0, 16, 0, 4, 0, 0, 0, 1, 0, 0, 0, 0, 0, 16, 0, 128, 0, 0, 170, 0, 56, 155, 113}
	x = append(x, extension...)
	x = append(x, b[36:]...)
	binary.LittleEndian.PutUint32(x[4:], uint32(len(x)-8))
	if _, e := audio.Parse(x); e != nil {
		t.Fatal(e)
	}
	x[44] = 3
	if _, e := audio.Parse(x); e == nil {
		t.Fatal("non-PCM subtype accepted")
	}
}
