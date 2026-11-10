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

	"repeats-detector/internal/domain"
	"repeats-detector/internal/fingerprint"
	"repeats-detector/internal/matching"
)

var ErrCandidateLimit = errors.New("candidate limit exceeded; no classification committed")

//go:embed schema.sql
var schema string

// Store хранит настройки поиска и записывает результаты в PostgreSQL.
type Store struct {
	Pool        *pgxpool.Pool
	MaxHits     int
	MaxPostings int
	Match       matching.Config
}

// Migrate создаёт таблицы под блокировкой, общей для всех запусков утилиты.
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

// Decide записывает классификацию и отпечатки в одной транзакции.
// Блокировка сериализует обработку внутри области поиска. В режиме READ COMMITTED
// запросы после её получения видят результаты предыдущей транзакции.
func (s *Store) Decide(ctx context.Context, t domain.Task, prints []domain.Print) (domain.Result, error) {
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
		if digest != t.TaskID {
			return r, domain.ErrConflict
		}
		e = json.Unmarshal(raw, &r)
		return r, e
	}
	if e != pgx.ErrNoRows {
		return r, e
	}
	// Берём время PostgreSQL после получения блокировки, чтобы не зависеть от часов процесса.
	var now time.Time
	if e = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); e != nil {
		return r, e
	}
	r = domain.Result{SchemaVersion: 1, File: t.File, TaskID: t.TaskID, TrackID: t.TaskID, ScopeID: t.ScopeID, Status: "insufficient_audio", AlgorithmVersion: fingerprint.Version, IndexedAt: now}
	hashes := make([]int64, len(prints))
	frames := make([]int32, len(prints))
	for i, p := range prints {
		hashes[i] = p.Hash
		frames[i] = int32(p.Frame)
	}
	if len(prints) > 0 {
		r.Status = "unique"
		// Перекрывающиеся окна ограничивают объём поиска за один запрос
		// и позволяют проверять совпадения на стыках. В индекс попадает весь файл.
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
				continue // Повторяем поиск с той же позиции и меньшим окном.
			}
			if err != nil {
				return r, err
			}
			candidate, err := matching.BestContext(ctx, hits, s.Match)
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
	if r.Match != nil {
		e = tx.QueryRow(ctx, "SELECT result->>'file' FROM rd_tasks WHERE scope_id=$1 AND task_id=$2",
			t.ScopeID, r.Match.OriginalTrackID).Scan(&r.Match.OriginalFile)
		if e != nil {
			return r, e
		}
	}
	raw, e = json.Marshal(r)
	if e != nil {
		return r, e
	}
	if _, e = tx.Exec(ctx, "INSERT INTO rd_tasks(scope_id,task_id,digest,result,created_at) VALUES($1,$2,$3,$4,$5)", t.ScopeID, t.TaskID, t.TaskID, raw, now); e != nil {
		return r, e
	}
	// Индексируем и частичные повторы, чтобы новые фрагменты участвовали в следующих поисках.
	if r.Status != "insufficient_audio" {
		_, e = tx.Exec(ctx, `INSERT INTO rd_prints(scope_id,task_id,track_id,version,hash,frame,created_at)
   SELECT $1,$2,$3,$4,x.hash,x.frame,$5 FROM unnest($6::bigint[],$7::integer[]) AS x(hash,frame)
   ON CONFLICT DO NOTHING`, t.ScopeID, t.TaskID, t.TaskID, fingerprint.Version, now, hashes, frames)
		if e != nil {
			return r, e
		}
	}
	return r, tx.Commit(ctx)
}

// informative проверяет разнообразие отпечатков и длительность сигнала между ними.
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
func (s *Store) candidates(ctx context.Context, tx pgx.Tx, t domain.Task, hashes []int64, frames []int32, now time.Time) ([]domain.Hit, error) {
	query, args := s.candidateSQL(t, hashes, frames, now)
	// Без кэшированного общего плана PostgreSQL строит план для текущей
	// области поиска, временной границы и размера массива отпечатков.
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
		// Служебная строка отмечает, что запрос отфильтровал частые хеши.
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
			// Частые хеши могут описывать весь повтор. Повторяем поиск с ними,
			// сохраняя общий лимит на число совпадений.
			unfiltered := *s
			unfiltered.MaxPostings = 0
			return unfiltered.candidates(ctx, tx, t, hashes, frames, now)
		}
	}
	return hits, nil
}

func (s *Store) candidateSQL(t domain.Task, hashes []int64, frames []int32, now time.Time) (string, []any) {
	// Для каждого хеша достаточно прочитать MaxPostings+1 строк, чтобы определить,
	// превышен ли лимит в текущей области. Слишком частый хеш исключается целиком.
	query := `SELECT p.hash,q.frame,p.frame,p.track_id,p.created_at
       FROM unnest($1::bigint[],$2::integer[]) AS q(hash,frame)
       JOIN rd_prints p ON p.hash=q.hash
       WHERE p.scope_id=$3 AND p.version=$4 AND p.created_at <= $5
       ORDER BY p.track_id,q.frame,p.frame LIMIT $6`
	args := []any{hashes, frames, t.ScopeID, fingerprint.Version, now, s.MaxHits + 1}
	if s.MaxPostings > 0 {
		query = `WITH query_prints AS MATERIALIZED (
          SELECT DISTINCT hash,frame FROM unnest($1::bigint[],$2::integer[]) AS q(hash,frame)
        ), postings AS MATERIALIZED (
          SELECT h.hash,p.frame,p.track_id,p.created_at,
                 count(*) OVER (PARTITION BY h.hash) AS frequency
          FROM (SELECT DISTINCT hash FROM query_prints) h
          CROSS JOIN LATERAL (
            SELECT frame,track_id,created_at FROM rd_prints
            WHERE scope_id=$3 AND version=$4 AND hash=h.hash AND created_at <= $5
            ORDER BY created_at
            LIMIT $7
          ) p
        )
        SELECT p.hash,q.frame,p.frame,p.track_id,p.created_at
        FROM query_prints q JOIN postings p ON p.hash=q.hash
        WHERE p.frequency < $7
        UNION ALL
        SELECT 0::bigint,-1::integer,-1::integer,''::text,$5::timestamptz
        WHERE EXISTS (SELECT 1 FROM postings WHERE frequency >= $7)
        LIMIT $6`
		args = append(args, s.MaxPostings+1)
	}
	return query, args
}

// CheckSchema проверяет версию таблиц при запуске с отключённой миграцией.
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
