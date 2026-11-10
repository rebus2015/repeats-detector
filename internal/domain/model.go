package domain

import (
	"errors"
	"time"
)

var ErrInvalid = errors.New("invalid input")
var ErrConflict = errors.New("task_id reused with different input")

// Task связывает файл с его идентификатором и областью поиска.
type Task struct {
	TaskID  string
	File    string
	ScopeID string
}

// Print хранит хеш пары спектральных пиков и номер её опорного кадра.
type Print struct {
	Hash  int64
	Frame int
}

// Hit связывает два кадра с одинаковым хешем в запросе и в индексе.
type Hit struct {
	CreatedAt     time.Time
	Hash          int64
	QueryFrame    int
	OriginalFrame int
	TrackID       string
}

// Match описывает выровненный фрагмент и его позиции в обеих записях.
type Match struct {
	OriginalFile       string  `json:"original_file"`
	OriginalTrackID    string  `json:"original_track_id"`
	QueryStartSec      float64 `json:"query_start_sec"`
	OriginalStartSec   float64 `json:"original_start_sec"`
	MatchedDurationSec float64 `json:"matched_duration_sec"`
	Score              float64 `json:"score"`
	Evidence           int     `json:"evidence"`
}

// Result сериализуется в JSON для PostgreSQL и стандартного вывода.
type Result struct {
	SchemaVersion    int       `json:"schema_version"`
	File             string    `json:"file"`
	TaskID           string    `json:"task_id"`
	TrackID          string    `json:"track_id"`
	ScopeID          string    `json:"scope_id"`
	Status           string    `json:"status"`
	AlgorithmVersion string    `json:"algorithm_version"`
	IndexedAt        time.Time `json:"indexed_at,omitempty"`
	Match            *Match    `json:"match_details,omitempty"`
}
