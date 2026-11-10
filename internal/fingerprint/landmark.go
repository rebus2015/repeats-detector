// Package fingerprint извлекает акустические отпечатки из PCM-аудио.
//
// Отпечаток описывает пару спектральных пиков: две частоты и расстояние между
// ними во времени. Абсолютная позиция хранится отдельно, поэтому обрезка начала
// записи не меняет смысл отпечатка. Устойчивость к шуму и перекодированию зависит
// от сохранности пиков; совпадение одного хеша само по себе не доказывает повтор.
// Последовательность и временное выравнивание проверяет пакет matching.
//
// FFT работает на исходной частоте выбранного пресета.
package fingerprint

import (
	"context"
	"fmt"
	"math"
	"math/cmplx"
	"sort"

	"repeats-detector/internal/audio"
	"repeats-detector/internal/domain"
)

// Оба пресета используют окно 64 мс и шаг 16 мс.
const FrameSeconds = 0.016

const maxFFTSize = 1024

// PairSpan ограничивает расстояние между пиками одной пары: 64 кадра = 1.024 с.
// Слишком далёкие пары увеличивают стоимость поиска целей и зависимость от
// наличия длинного непрерывного фрагмента. На один пик создаётся не более 6 пар.
const PairSpan = 64

// radius — временной радиус поиска локального максимума: ±3 кадра = ±48 мс.
// Для решения о пике нужны три предыдущих и три следующих спектра, всего семь.
// Частотный радиус в проверке ниже также равен 3, но задан отдельно в FFT-бинах.
const radius = 3

// Параметры алгоритма. Изменение правил формирования отпечатков требует
// новой версии; эти значения не являются настройками рабочего окружения.
const (
	frequencyRadius      = 3 // частотное окружение пика в FFT-бинах
	firstSpectrumBin     = 4 // пропускаем DC и самые низкие частоты
	firstPeakBin         = firstSpectrumBin + frequencyRadius
	minMeanSquareEnergy  = 1e-7
	minPeakPower         = 1e-8
	relativePeakPower    = 0.04
	maxPeaksPerFrame     = 3
	minPairFrames        = 4
	maxPairsPerPeak      = 6
	pairTimeQuantization = 2
	targetFrequencyShift = 10
	anchorFrequencyShift = 20
	defaultBinWidth      = 2
	maxBinWidth          = 16
)

// Ограничения ресурсов и частота проверки отмены не меняют формат отпечатков.
const (
	defaultMaxPrints       = 1000000
	spectrumCancelInterval = 64
	pairingCancelInterval  = 128
	initialPeakCapacity    = 16 // начальная ёмкость, не ограничение числа пиков
)

type powerSpectrum [maxFFTSize / 2]float64
type spectrumRing [2*radius + 1]powerSpectrum

// peak — локальный максимум спектральной мощности. bin обозначает частотный
// индекс FFT, frame — временной индекс окна, power — ненормированную мощность.
type peak struct {
	bin, frame int
	power      float64
}

// Extractor хранит окно Ханна и ограничения извлечения.
// Можно использовать один экземпляр параллельно, если после создания его поля
// не изменяются: рабочие массивы создаются отдельно для каждого вызова Extract.
type Extractor struct {
	// window сглаживает края окна, уменьшая спектральную утечку при FFT.
	window  []float64
	version string

	// BinWidth — ширина частотной группы в FFT-бинах. Значение 2 даёт шаг
	// 31.25 Гц; квантование повышает терпимость к небольшим изменениям спектра,
	// но объединяет разные пики и повышает риск случайного совпадения.
	// MaxPrints ограничивает размер результата, а не параметры самого хеша.
	BinWidth, MaxPrints int
}

// New8kHz создаёт извлекатель для A-law 8 кГц.
func New8kHz() *Extractor {
	return newExtractor(512, "landmark-v3-alaw-8khz-fft512-hop128")
}

// New16kHz создаёт извлекатель для линейного PCM 16 кГц.
func New16kHz() *Extractor {
	return newExtractor(1024, "landmark-v3-s16le-16khz-fft1024-hop256")
}

// Version отделяет несовместимые отпечатки в поисковом индексе.
func (e *Extractor) Version() string { return e.version }

func newExtractor(size int, version string) *Extractor {
	e := &Extractor{
		window:    make([]float64, size),
		version:   version,
		BinWidth:  defaultBinWidth,
		MaxPrints: defaultMaxPrints,
	}
	// Симметричное окно Ханна имеет нулевые значения на обоих краях кадра.
	// Знаменатель size-1 важен: другая форма окна может изменить отбор пиков.
	for i := range e.window {
		e.window[i] = 0.5 - 0.5*math.Cos(2*math.Pi*float64(i)/float64(size-1))
	}
	return e
}

// fft выполняет прямое ненормированное radix-2 преобразование на месте.
// Длина a должна быть степенью двойки; Extract передаёт окно выбранного пресета.
// После преобразования элементы содержат комплексные частотные коэффициенты.
func fft(a []complex128) {
	n := len(a)
	// Перестановка по обращённым битам подготавливает данные для итеративных
	// «бабочек» Cooley–Tukey без дополнительного массива.
	for i, j := 1, 0; i < n; i++ {
		bit := n >> 1
		for ; j&bit != 0; bit >>= 1 {
			j ^= bit
		}
		j ^= bit
		if i < j {
			a[i], a[j] = a[j], a[i]
		}
	}
	// На каждом шаге объединяем преобразования половинок длины width/2.
	// Отрицательный знак фазы соответствует прямому FFT; нормировка не нужна,
	// поскольку далее сравниваются мощности, рассчитанные одинаковым способом.
	for width := 2; width <= n; width *= 2 {
		root := cmplx.Exp(complex(0, -2*math.Pi/float64(width)))
		for start := 0; start < n; start += width {
			w := complex(1, 0)
			for j := 0; j < width/2; j++ {
				u := a[start+j]
				v := w * a[start+j+width/2]
				a[start+j] = u + v
				a[start+j+width/2] = u - v
				w *= root
			}
		}
	}
}

// Extract возвращает хеши пар пиков и позиции их опорных кадров.
// На вход подаются декодированные линейные отсчёты монофонического аудио выбранного пресета.
// Метод читает исходный буфер и не изменяет его; вызывающий код должен сохранять
// буфер неизменным до возврата. Результат уже не ссылается на исходное аудио.
//
// Неполное последнее окно и крайние кадры без полного временного окружения
// не анализируются как опорные пики. Для короткого или тихого аудио возможен
// пустой результат без ошибки — итоговую информативность оценивает хранилище при классификации.
// Результат ограничен первыми MaxPrints отпечатками. При отмене контекста
// метод возвращает ошибку.
func (e *Extractor) Extract(ctx context.Context, pcm audio.PCM) ([]domain.Print, error) {
	if e.BinWidth < 1 || e.BinWidth > maxBinWidth || e.MaxPrints < 1 {
		return nil, fmt.Errorf("invalid extractor configuration")
	}
	if pcm.Len() < len(e.window) {
		return nil, nil
	}
	peaks, err := e.extractPeaks(ctx, pcm)
	if err != nil {
		return nil, err
	}
	return e.pairPeaks(ctx, peaks)
}

// extractPeaks хранит только окружение, необходимое для локальных максимумов.
// Первые и последние radius кадров не имеют полного окружения и не дают опорных пиков.
func (e *Extractor) extractPeaks(ctx context.Context, pcm audio.PCM) ([]peak, error) {
	size := len(e.window)
	hop := size / 4
	count := 1 + (pcm.Len()-size)/hop
	var ring spectrumRing
	scratch := make([]complex128, size)
	peaks := make([]peak, 0, count)
	for frame := 0; frame < count; frame++ {
		if frame%spectrumCancelInterval == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		e.computeSpectrum(pcm, frame*hop, scratch, &ring[frame%len(ring)])
		if frame < 2*radius {
			continue
		}
		peaks = append(peaks, ring.framePeaks(frame-radius, size/2)...)
	}
	return peaks, nil
}

// computeSpectrum проверяет энергию исходного сигнала и вычисляет мощность FFT
// после применения окна Ханна. Очистка слота даже для тишины исключает старые пики.
func (e *Extractor) computeSpectrum(pcm audio.PCM, offset int, scratch []complex128, out *powerSpectrum) {
	energy := 0.0
	for i := 0; i < len(scratch); i++ {
		v := pcm.Sample(offset + i)
		energy += v * v
		scratch[i] = complex(v*e.window[i], 0)
	}
	clear(out[:])
	if energy/float64(len(scratch)) < minMeanSquareEnergy {
		return
	}
	fft(scratch)
	for i := firstSpectrumBin; i < len(scratch)/2; i++ {
		out[i] = real(scratch[i])*real(scratch[i]) + imag(scratch[i])*imag(scratch[i])
	}
}

// framePeaks выбирает сильнейшие локальные максимумы.
// Относительный и абсолютный пороги отсекают слабые пики.
func (ring *spectrumRing) framePeaks(center, bins int) []peak {
	middle := &ring[center%len(ring)]
	maximum := 0.0
	for _, v := range middle[:bins] {
		maximum = math.Max(maximum, v)
	}
	candidates := make([]peak, 0, initialPeakCapacity)
	for bin := firstPeakBin; bin < bins-frequencyRadius-1; bin++ {
		value := middle[bin]
		if value <= maximum*relativePeakPower || value < minPeakPower {
			continue
		}
		if ring.isLocalPeak(center, bin) {
			candidates = append(candidates, peak{bin: bin, frame: center, power: value})
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].power > candidates[j].power })
	if len(candidates) > maxPeaksPerFrame {
		candidates = candidates[:maxPeaksPerFrame]
	}
	return candidates
}

// isLocalPeak на плато предпочитает ранний кадр, затем меньшую частоту.
// Требует полного временного окружения и запаса по границам частотных бинов.
func (ring *spectrumRing) isLocalPeak(center, bin int) bool {
	value := ring[center%len(ring)][bin]
	for dt := -radius; dt <= radius; dt++ {
		for df := -frequencyRadius; df <= frequencyRadius; df++ {
			if dt == 0 && df == 0 {
				continue
			}
			other := ring[(center+dt)%len(ring)][bin+df]
			if other > value || (other == value && (dt < 0 || (dt == 0 && df < 0))) {
				return false
			}
		}
	}
	return true
}

// pairPeaks выбирает первые допустимые будущие цели в порядке времени,
// а не самые сильные. После заполнения лимита формирование пар останавливается.
func (e *Extractor) pairPeaks(ctx context.Context, peaks []peak) ([]domain.Print, error) {
	out := make([]domain.Print, 0, min(e.MaxPrints, len(peaks)*maxPairsPerPeak))
	for i, a := range peaks {
		if i%pairingCancelInterval == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		paired := 0
		for j := i + 1; j < len(peaks); j++ {
			b := peaks[j]
			delta := b.frame - a.frame
			if delta < minPairFrames {
				continue
			}
			if delta > PairSpan {
				break
			}
			out = append(out, domain.Print{Hash: e.pairHash(a.bin, b.bin, delta), Frame: a.frame})
			if len(out) == e.MaxPrints {
				return out, nil
			}
			paired++
			if paired == maxPairsPerPeak {
				break
			}
		}
	}
	return out, nil
}

// pairHash упаковывает две частотные группы и временной интервал в непересекающиеся
// поля: [anchor << 20 | target << 10 | delta/2]. Абсолютная позиция хранится отдельно.
func (e *Extractor) pairHash(anchorBin, targetBin, delta int) int64 {
	return int64((anchorBin/e.BinWidth)<<anchorFrequencyShift | (targetBin/e.BinWidth)<<targetFrequencyShift | (delta / pairTimeQuantization))
}
