// Package fingerprint извлекает акустические отпечатки из PCM-аудио.
//
// Отпечаток описывает пару спектральных пиков: две частоты и расстояние между
// ними во времени. Абсолютная позиция хранится отдельно, поэтому обрезка начала
// записи не меняет смысл отпечатка. Устойчивость к шуму и перекодированию зависит
// от сохранности пиков; совпадение одного хеша само по себе не доказывает повтор.
// Последовательность и временное выравнивание проверяет пакет matching.
//
// Параметры v2 — экспериментальный компромисс, проверенный на небольшом корпусе
// речи. Полной оптимизации параметров FFT, отбора пиков и построения пар не было.
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

// Version отделяет несовместимые отпечатки в поисковом индексе.
// При изменении правил извлечения или квантования рабочему алгоритму нужна
// новая версия. BinWidth=4 используется только как отдельный вариант оценки;
// вызывающий код не должен смешивать его отпечатки с рабочим индексом v2.
const Version = "landmark-v2-temporal-fft1024-hop256"

// Size — число отсчётов в окне FFT: 1024 / 16000 = 64 мс.
// Частотный шаг равен 16000 / 1024 = 15.625 Гц. Степень двойки позволяет
// использовать radix-2 FFT; большее окно улучшает частотное разрешение,
// но ухудшает локализацию быстрых изменений речи.
const Size = 1024

// Hop — шаг анализа в отсчётах: 256 / 16000 = 16 мс, перекрытие окон 75%.
// Плотная сетка уменьшает чувствительность к позиции обрезки, однако произвольный
// сдвиг аудио всё равно меняет содержимое окон: идентичность хешей не гарантируется.
const Hop = 256

// FrameSeconds переводит индекс кадра в секунды относительно начала PCM.
const FrameSeconds = float64(Hop) / audio.SampleRate

// PairSpan ограничивает расстояние между пиками одной пары: 64 кадра = 1.024 с.
// Слишком далёкие пары увеличивают стоимость поиска целей и зависимость от
// наличия длинного непрерывного фрагмента. На один пик создаётся не более 6 пар.
const PairSpan = 64

// radius — временной радиус поиска локального максимума: ±3 кадра = ±48 мс.
// Для решения о пике нужны три предыдущих и три следующих спектра, всего семь.
// Частотный радиус в проверке ниже также равен 3, но задан отдельно в FFT-бинах.
const radius = 3

// Параметры алгоритма v2. Изменение правил формирования отпечатков требует
// новой Version; эти значения не являются настройками рабочего окружения.
const (
	frequencyRadius      = 3 // частотное окружение пика в FFT-бинах
	firstSpectrumBin     = 4 // пропускаем DC и самые низкие частоты
	firstPeakBin         = firstSpectrumBin + frequencyRadius
	peakBinEnd           = Size/2 - frequencyRadius - 1 // верхняя граница не включается; сохраняет запас v2
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

type powerSpectrum [Size / 2]float64
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
	window [Size]float64

	// BinWidth — ширина частотной группы в FFT-бинах. Значение 2 даёт шаг
	// 31.25 Гц; квантование повышает терпимость к небольшим изменениям спектра,
	// но объединяет разные пики и повышает риск случайного совпадения.
	// MaxPrints ограничивает размер результата, а не параметры самого хеша.
	BinWidth, MaxPrints int
}

// New создаёт рабочую конфигурацию v2: частотное квантование по 2 бина
// и защитный лимит в миллион отпечатков. Лимит можно задать до начала обработки.
func New() *Extractor {
	e := &Extractor{BinWidth: defaultBinWidth, MaxPrints: defaultMaxPrints}
	// Симметричное окно Ханна имеет нулевые значения на обоих краях кадра.
	// Знаменатель Size-1 важен: другая форма окна может изменить отбор пиков.
	for i := range e.window {
		e.window[i] = 0.5 - 0.5*math.Cos(2*math.Pi*float64(i)/float64(Size-1))
	}
	return e
}

// fft выполняет прямое ненормированное radix-2 преобразование на месте.
// Длина a должна быть степенью двойки; Extract всегда передаёт Size элементов.
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
// PCM должен быть предварительно проверен: 16 кГц, mono, signed 16-bit little-endian.
// Метод читает исходный буфер и не изменяет его; вызывающий код должен сохранять
// буфер неизменным до возврата. Результат уже не ссылается на исходное аудио.
//
// Неполное последнее окно и крайние кадры без полного временного окружения
// не анализируются как опорные пики. Для короткого или тихого аудио возможен
// пустой результат без ошибки — итоговую информативность оценивает use case.
// При отмене контекста или превышении лимита частичный результат не возвращается.
func (e *Extractor) Extract(ctx context.Context, pcm audio.PCM) ([]domain.Print, error) {
	if e.BinWidth < 1 || e.BinWidth > maxBinWidth || e.MaxPrints < 1 {
		return nil, fmt.Errorf("invalid extractor configuration")
	}
	if pcm.Len() < Size {
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
	count := 1 + (pcm.Len()-Size)/Hop
	var ring spectrumRing
	var scratch [Size]complex128
	peaks := make([]peak, 0, count)
	for frame := 0; frame < count; frame++ {
		if frame%spectrumCancelInterval == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		e.computeSpectrum(pcm, frame, &scratch, &ring[frame%len(ring)])
		if frame < 2*radius {
			continue
		}
		peaks = append(peaks, ring.framePeaks(frame-radius)...)
	}
	return peaks, nil
}

// computeSpectrum проверяет энергию исходного сигнала и вычисляет мощность FFT
// после применения окна Ханна. Очистка слота даже для тишины исключает старые пики.
func (e *Extractor) computeSpectrum(pcm audio.PCM, frame int, scratch *[Size]complex128, out *powerSpectrum) {
	energy := 0.0
	for i := 0; i < Size; i++ {
		v := pcm.Sample(frame*Hop + i)
		energy += v * v
		scratch[i] = complex(v*e.window[i], 0)
	}
	clear(out[:])
	if energy/Size < minMeanSquareEnergy {
		return
	}
	fft(scratch[:])
	for i := firstSpectrumBin; i < Size/2; i++ {
		out[i] = real(scratch[i])*real(scratch[i]) + imag(scratch[i])*imag(scratch[i])
	}
}

// framePeaks выбирает сильнейшие локальные максимумы, сохраняя порядок v2
// при равных мощностях. Относительный и абсолютный пороги отсекают слабые пики.
func (ring *spectrumRing) framePeaks(center int) []peak {
	middle := &ring[center%len(ring)]
	maximum := 0.0
	for _, v := range middle {
		maximum = math.Max(maximum, v)
	}
	candidates := make([]peak, 0, initialPeakCapacity)
	for bin := firstPeakBin; bin < peakBinEnd; bin++ {
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
// а не самые сильные. При отмене или превышении лимита частичный результат отбрасывается.
func (e *Extractor) pairPeaks(ctx context.Context, peaks []peak) ([]domain.Print, error) {
	out := make([]domain.Print, 0, len(peaks)*maxPairsPerPeak)
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
			if len(out) >= e.MaxPrints {
				return nil, fmt.Errorf("%w: fingerprint limit exceeded", domain.ErrInvalid)
			}
			out = append(out, domain.Print{Hash: e.pairHash(a.bin, b.bin, delta), Frame: a.frame})
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
