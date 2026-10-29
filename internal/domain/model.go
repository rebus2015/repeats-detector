package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrInvalid = errors.New("invalid input")
var ErrConflict = errors.New("task_id reused with different input")

type Task struct {
	SchemaVersion int    `json:"schema_version"`
	TaskID        string `json:"task_id"`
	TrackID       string `json:"track_id"`
	S3URL         string `json:"s3_url"`
	ScopeID       string `json:"scope_id"`
}

func (t *Task) Validate() error {
	if t.SchemaVersion == 0 {
		t.SchemaVersion = 1
	}
	if t.ScopeID == "" {
		t.ScopeID = "default"
	}
	if t.SchemaVersion != 1 {
		return fmt.Errorf("%w: unsupported schema", ErrInvalid)
	}
	for _, s := range []string{t.TaskID, t.TrackID, t.ScopeID} {
		if strings.TrimSpace(s) == "" || len(s) > 256 || strings.ContainsRune(s, 0) {
			return fmt.Errorf("%w: invalid identifier", ErrInvalid)
		}
	}
	if t.S3URL == "" || len(t.S3URL) > 4096 {
		return fmt.Errorf("%w: s3_url required", ErrInvalid)
	}
	return nil
}
func (t Task) Digest() string {
	b, _ := json.Marshal(t)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (t Task) EventID() string {
	h := sha256.Sum256([]byte(t.ScopeID + "\x00" + t.TaskID))
	return hex.EncodeToString(h[:])
}

type Print struct {
	Hash  int64
	Frame int
}
type Hit struct {
	CreatedAt     time.Time
	Hash          int64
	QueryFrame    int
	OriginalFrame int
	TrackID       string
}
type Match struct {
	OriginalTrackID    string  `json:"original_track_id"`
	QueryStartSec      float64 `json:"query_start_sec"`
	OriginalStartSec   float64 `json:"original_start_sec"`
	MatchedDurationSec float64 `json:"matched_duration_sec"`
	Score              float64 `json:"score"`
	Evidence           int     `json:"evidence"`
}
type Result struct {
	SchemaVersion    int       `json:"schema_version"`
	EventID          string    `json:"event_id"`
	TaskID           string    `json:"task_id"`
	TrackID          string    `json:"track_id"`
	ScopeID          string    `json:"scope_id"`
	Status           string    `json:"status"`
	AlgorithmVersion string    `json:"algorithm_version"`
	IndexedAt        time.Time `json:"indexed_at,omitempty"`
	Match            *Match    `json:"match_details,omitempty"`
}
