package audio

import (
	"encoding/binary"
	"fmt"

	"repeats-detector/internal/domain"
)

// PCM хранит декодированные знаковые линейные отсчёты.
type PCM []int16

func (p PCM) Len() int { return len(p) }

// Sample нормирует значение отсчёта перед спектральным анализом.
func (p PCM) Sample(i int) float64 {
	return float64(p[i]) / 32768
}

// DecodeALaw преобразует каждый байт G.711 A-law в линейный отсчёт.
func DecodeALaw(data []byte) (PCM, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty A-law audio", domain.ErrInvalid)
	}
	pcm := make(PCM, len(data))
	for i, code := range data {
		// Восстанавливаем инвертированные биты перед разбором знака, сегмента и уровня.
		code ^= 0x55
		segment := (code >> 4) & 7
		value := int16(code&15)<<4 | 8
		if segment > 0 {
			value = (value + 256) << (segment - 1)
		}
		if code&0x80 == 0 {
			value = -value
		}
		pcm[i] = value
	}
	return pcm, nil
}

// DecodeS16LE читает знаковые 16-битные линейные отсчёты без заголовка.
func DecodeS16LE(data []byte) (PCM, error) {
	if len(data) == 0 || len(data)%2 != 0 {
		return nil, fmt.Errorf("%w: S16LE requires a non-empty sequence of complete 16-bit samples", domain.ErrInvalid)
	}
	pcm := make(PCM, len(data)/2)
	for i := range pcm {
		pcm[i] = int16(binary.LittleEndian.Uint16(data[2*i:]))
	}
	return pcm, nil
}
