package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"repeats-detector/internal/domain"
	"repeats-detector/internal/fingerprint"
	"repeats-detector/internal/matching"
)

var ErrCandidateLimit = errors.New("candidate limit exceeded; no classification committed")

//go:embed schema.sql
var schema string

type Store struct {
	Pool                                          *pgxpool.Pool
	Window                                        time.Duration
	MaxHits                                       int
	CleanupPrintsBatchSize, CleanupTasksBatchSize int
	// MaxPostings excludes very frequent hashes within this scope and search window.
	// Zero preserves the unfiltered legacy search, useful for controlled comparisons.
	MaxPostings int
	Match       matching.Config
}

func (s *Store) Migrate(ctx context.Context) error {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(81329122)"); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, schema); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) Existing(ctx context.Context, t domain.Task) (*domain.Result, error) {
	var raw []byte
	var digest string
	e := s.Pool.QueryRow(ctx, "SELECT digest,result FROM rd_tasks WHERE scope_id=$1 AND task_id=$2", t.ScopeID, t.TaskID).Scan(&digest, &raw)
	if e == pgx.ErrNoRows {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	if digest != t.Digest() {
		return nil, domain.ErrConflict
	}
	var r domain.Result
	e = json.Unmarshal(raw, &r)
	return &r, e
}

// Decide uses READ COMMITTED and acquires a scope lock before searching. The next
// statement sees the previous lock holder's commit, including its fingerprints.
func (s *Store) Decide(ctx context.Context, t domain.Task, prints []domain.Print, headers map[string]string) (domain.Result, error) {
	r := domain.Result{}
	tx, e := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if e != nil {
		return r, e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 81723))", t.ScopeID); e != nil {
		return r, e
	}
	var raw []byte
	var digest string
	e = tx.QueryRow(ctx, "SELECT digest,result FROM rd_tasks WHERE scope_id=$1 AND task_id=$2", t.ScopeID, t.TaskID).Scan(&digest, &raw)
	if e == nil {
		if digest != t.Digest() {
			return r, domain.ErrConflict
		}
		e = json.Unmarshal(raw, &r)
		return r, e
	}
	if e != pgx.ErrNoRows {
		return r, e
	}
	// Use database time after waiting for the scope lock, avoiding process clock skew.
	var now time.Time
	if e = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); e != nil {
		return r, e
	}
	r = domain.Result{SchemaVersion: 1, EventID: t.EventID(), TaskID: t.TaskID, TrackID: t.TrackID, ScopeID: t.ScopeID, Status: "insufficient_audio", AlgorithmVersion: fingerprint.Version, IndexedAt: now}
	hashes := make([]int64, len(prints))
	frames := make([]int32, len(prints))
	for i, p := range prints {
		hashes[i] = p.Hash
		frames[i] = int32(p.Frame)
	}
	if len(prints) > 0 {
		r.Status = "unique"
		// Bound search memory independently of file duration. Overlapping windows
		// retain matches crossing a boundary. Full-file prints are still indexed.
		ordered := append([]domain.Print(nil), prints...)
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].Frame < ordered[j].Frame })
		width := int(math.Ceil(math.Max(10, 2*s.Match.MinDuration+s.Match.MaxGap) / fingerprint.FrameSeconds))
		overlap := int(math.Ceil((s.Match.MinDuration + s.Match.MaxGap) / fingerprint.FrameSeconds))
		for start := 0; start < len(ordered); {
			end := sort.Search(len(ordered), func(i int) bool { return ordered[i].Frame >= ordered[start].Frame+width })
			batch := ordered[start:end]
			bh, bf := make([]int64, len(batch)), make([]int32, len(batch))
			for i, p := range batch {
				bh[i], bf[i] = p.Hash, int32(p.Frame)
			}
			hits, err := s.candidates(ctx, tx, t, bh, bf, now)
			minWidth := int(math.Ceil((2*s.Match.MinDuration + s.Match.MaxGap) / fingerprint.FrameSeconds))
			if errors.Is(err, ErrCandidateLimit) && width > minWidth {
				width = max(minWidth, width/2)
				continue // Retry this position with a smaller, still overlapping window.
			}
			if err != nil {
				return r, err
			}
			verifyCtx, span := otel.Tracer("repeats-detector").Start(ctx, "Match_Verify")
			candidate, err := matching.BestContext(verifyCtx, hits, s.Match)
			if err != nil {
				span.RecordError(err)
			}
			span.End()
			if err != nil {
				return r, err
			}
			if candidate != nil && (r.Match == nil || candidate.Evidence > r.Match.Evidence) {
				r.Match = candidate
			}
			if end == len(ordered) {
				break
			}
			next := sort.Search(len(ordered), func(i int) bool { return ordered[i].Frame >= ordered[start].Frame+width-overlap })
			if next <= start {
				next = end
			}
			start = next
		}
		if r.Match != nil {
			r.Status = "duplicate"
		}
		if !informative(prints, s.Match) {
			r.Status = "insufficient_audio"
			r.Match = nil
		}
	}
	raw, e = json.Marshal(r)
	if e != nil {
		return r, e
	}
	if _, e = tx.Exec(ctx, "INSERT INTO rd_tasks(scope_id,task_id,digest,result,created_at) VALUES($1,$2,$3,$4,$5)", t.ScopeID, t.TaskID, t.Digest(), raw, now); e != nil {
		return r, e
	}
	// Index partial repeats too, preserving new audio for future queries.
	if r.Status != "insufficient_audio" {
		indexCtx, indexSpan := otel.Tracer("repeats-detector").Start(ctx, "Index_Write")
		_, e = tx.Exec(indexCtx, `INSERT INTO rd_prints(scope_id,task_id,track_id,version,hash,frame,created_at)
   SELECT $1,$2,$3,$4,x.hash,x.frame,$5 FROM unnest($6::bigint[],$7::integer[]) AS x(hash,frame)
   ON CONFLICT DO NOTHING`, t.ScopeID, t.TaskID, t.TrackID, fingerprint.Version, now, hashes, frames)
		if e != nil {
			indexSpan.RecordError(e)
		}
		indexSpan.End()
		if e != nil {
			return r, e
		}
	}
	hdr, e := json.Marshal(headers)
	if e != nil {
		return r, e
	}
	if _, e = tx.Exec(ctx, "INSERT INTO rd_outbox(event_id,scope_id,task_id,body,headers,created_at) VALUES($1,$2,$3,$4,$5,$6)", r.EventID, t.ScopeID, t.TaskID, raw, hdr, now); e != nil {
		return r, e
	}
	return r, tx.Commit(ctx)
}
func informative(p []domain.Print, c matching.Config) bool {
	frames := map[int]bool{}
	hashes := map[int64]bool{}
	lo, hi := int(^uint(0)>>1), 0
	for _, v := range p {
		frames[v.Frame] = true
		hashes[v.Hash] = true
		if v.Frame < lo {
			lo = v.Frame
		}
		if v.Frame > hi {
			hi = v.Frame
		}
	}
	return len(frames) >= c.MinFrames && len(hashes) >= c.MinFrames && float64(hi-lo)*fingerprint.FrameSeconds >= c.MinDuration
}
func (s *Store) Delivered(ctx context.Context, id string) (bool, error) {
	var yes bool
	e := s.Pool.QueryRow(ctx, "SELECT delivered_at IS NOT NULL FROM rd_outbox WHERE event_id=$1", id).Scan(&yes)
	return yes, e
}

type PublishFunc func(context.Context, string, []byte, map[string]string) error

// A row lock prevents concurrent outbox publishers from handling the same event.
// A crash after broker confirmation still permits re-publication with the same ID.
func (s *Store) PublishOne(ctx context.Context, publish PublishFunc) (bool, error) {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return false, e
	}
	defer tx.Rollback(ctx)
	var id string
	var body, hdr []byte
	e = tx.QueryRow(ctx, "SELECT event_id,body,headers FROM rd_outbox WHERE delivered_at IS NULL ORDER BY created_at,event_id FOR UPDATE SKIP LOCKED LIMIT 1").Scan(&id, &body, &hdr)
	if e == pgx.ErrNoRows {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	headers := map[string]string{}
	if e = json.Unmarshal(hdr, &headers); e != nil {
		return false, e
	}
	if e = publish(ctx, id, body, headers); e != nil {
		return false, e
	}
	if _, e = tx.Exec(ctx, "UPDATE rd_outbox SET delivered_at=clock_timestamp() WHERE event_id=$1", id); e != nil {
		return false, e
	}
	return true, tx.Commit(ctx)
}
func (s *Store) Cleanup(ctx context.Context, indexAge, taskAge time.Duration) error {
	ctx, span := otel.Tracer("repeats-detector").Start(ctx, "Retention_Cleanup")
	defer span.End()
	// Commit bounded batches and continue until drained or the caller's time
	// budget is exhausted. A single batch per tick cannot keep up with a long-
	// recording index; search expiry itself is independent of physical cleanup.
	printsBatch, tasksBatch := s.CleanupPrintsBatchSize, s.CleanupTasksBatchSize
	if printsBatch == 0 {
		printsBatch = 50000
	}
	if tasksBatch == 0 {
		tasksBatch = 1000
	}
	statements := []struct {
		sql   string
		age   time.Duration
		batch int64
	}{
		{`DELETE FROM rd_prints WHERE ctid IN
       (SELECT ctid FROM rd_prints WHERE created_at<clock_timestamp()-$1::interval LIMIT $2)`, indexAge, int64(printsBatch)},
		{`DELETE FROM rd_tasks WHERE (scope_id,task_id) IN
       (SELECT t.scope_id,t.task_id FROM rd_tasks t WHERE t.created_at<clock_timestamp()-$1::interval
        AND NOT EXISTS(SELECT 1 FROM rd_outbox o WHERE o.scope_id=t.scope_id AND o.task_id=t.task_id AND o.delivered_at IS NULL) LIMIT $2)`, taskAge, int64(tasksBatch)},
	}
	for _, statement := range statements {
		for {
			tag, e := s.Pool.Exec(ctx, statement.sql, fmt.Sprintf("%f seconds", statement.age.Seconds()), statement.batch)
			if e != nil {
				return e
			}
			if tag.RowsAffected() < statement.batch {
				break
			}
		}
	}
	return nil
}

func (s *Store) candidates(ctx context.Context, tx pgx.Tx, t domain.Task, hashes []int64, frames []int32, now time.Time) ([]domain.Hit, error) {
	ctx, span := otel.Tracer("repeats-detector").Start(ctx, "Candidate_Search")
	defer span.End()
	query, args := s.candidateSQL(t, hashes, frames, now)
	// The scope and array lengths vary by orders of magnitude. A cached generic
	// plan can select an expiry-index scan for every hash after a few executions.
	// Use an unnamed extended-protocol statement so PostgreSQL plans this batch
	// with its actual scope, time bounds and query-array cardinality.
	queryArgs := append([]any{pgx.QueryExecModeExec}, args...)
	rows, err := tx.Query(ctx, query, queryArgs...)
	if err != nil {
		return nil, err
	}
	hits := make([]domain.Hit, 0)
	excluded := false
	for rows.Next() {
		var h domain.Hit
		if err = rows.Scan(&h.Hash, &h.QueryFrame, &h.OriginalFrame, &h.TrackID, &h.CreatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		if h.QueryFrame < 0 {
			excluded = true
			continue
		}
		hits = append(hits, h)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(hits) > s.MaxHits {
		return nil, ErrCandidateLimit
	}
	if excluded {
		candidate, err := matching.BestContext(ctx, hits, s.Match)
		if err != nil {
			return nil, err
		}
		if candidate == nil {
			// Even a minority of common hashes can contain the entire repeated
			// segment. Retry all evidence together under the same hard hit limit.
			span.SetAttributes(attribute.Bool("audio.frequent_hash_fallback", true))
			unfiltered := *s
			unfiltered.MaxPostings = 0
			return unfiltered.candidates(ctx, tx, t, hashes, frames, now)
		}
	}
	span.SetAttributes(attribute.Int("audio.candidate_hits", len(hits)))
	return hits, nil
}

// Backlog is cheap even when delivered history is large: the pending partial
// index serves both count and oldest age. Age is computed by database time.
func (s *Store) Backlog(ctx context.Context) (int64, float64, error) {
	var count int64
	var age float64
	e := s.Pool.QueryRow(ctx, `SELECT count(*),COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-min(created_at)),0)
 FROM rd_outbox WHERE delivered_at IS NULL`).Scan(&count, &age)
	return count, age, e
}

func (s *Store) candidateSQL(t domain.Task, hashes []int64, frames []int32, now time.Time) (string, []any) {
	// Materialize at most MaxPostings+1 rows per distinct query hash. A hash
	// exceeding the bound is excluded entirely, never arbitrarily truncated.
	// The cap depends on the active scope/window, not on unrelated tenants.
	query := `SELECT p.hash,q.frame,p.frame,p.track_id,p.created_at
       FROM unnest($1::bigint[],$2::integer[]) AS q(hash,frame)
       JOIN rd_prints p ON p.hash=q.hash
       WHERE p.scope_id=$3 AND p.version=$4 AND p.created_at >= $5 AND p.created_at <= $6
       ORDER BY p.track_id,q.frame,p.frame LIMIT $7`
	args := []any{hashes, frames, t.ScopeID, fingerprint.Version, now.Add(-s.Window), now, s.MaxHits + 1}
	if s.MaxPostings > 0 {
		query = `WITH query_prints AS MATERIALIZED (
          SELECT DISTINCT hash,frame FROM unnest($1::bigint[],$2::integer[]) AS q(hash,frame)
        ), postings AS MATERIALIZED (
          SELECT h.hash,p.frame,p.track_id,p.created_at,
                 count(*) OVER (PARTITION BY h.hash) AS frequency
          FROM (SELECT DISTINCT hash FROM query_prints) h
          CROSS JOIN LATERAL (
            SELECT frame,track_id,created_at FROM rd_prints
            WHERE scope_id=$3 AND version=$4 AND hash=h.hash AND created_at >= $5 AND created_at <= $6
            ORDER BY created_at
            LIMIT $8
          ) p
        )
        SELECT p.hash,q.frame,p.frame,p.track_id,p.created_at
        FROM query_prints q JOIN postings p ON p.hash=q.hash
        WHERE p.frequency < $8
        UNION ALL
        SELECT 0::bigint,-1::integer,-1::integer,''::text,$6::timestamptz
        WHERE EXISTS (SELECT 1 FROM postings WHERE frequency >= $8)
        LIMIT $7`
		args = append(args, s.MaxPostings+1)
	}
	return query, args
}

// ExplainCandidates executes a bounded read-only diagnostic query. Operators
// should use a test scope: ANALYZE actually runs candidate retrieval.
func (s *Store) ExplainCandidates(ctx context.Context, scope string, prints []domain.Print) ([]string, error) {
	hashes, frames := make([]int64, len(prints)), make([]int32, len(prints))
	for i, p := range prints {
		hashes[i], frames[i] = p.Hash, int32(p.Frame)
	}
	q, args := s.candidateSQL(domain.Task{ScopeID: scope}, hashes, frames, time.Now())
	rows, e := s.Pool.Query(ctx, "EXPLAIN (ANALYZE,BUFFERS,TIMING OFF) "+q, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	lines := []string{}
	for rows.Next() {
		var line string
		if e = rows.Scan(&line); e != nil {
			return nil, e
		}
		lines = append(lines, line)
	}
	return lines, rows.Err()
}

// CheckSchema allows the runtime role to start without DDL permissions.
func (s *Store) CheckSchema(ctx context.Context) error {
	var version int
	if e := s.Pool.QueryRow(ctx, "SELECT max(version) FROM rd_schema").Scan(&version); e != nil {
		return e
	}
	if version != 1 {
		return fmt.Errorf("unsupported database schema version %d", version)
	}
	return nil
}
