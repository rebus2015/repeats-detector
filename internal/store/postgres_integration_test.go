//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"repeats-detector/internal/domain"
	"repeats-detector/internal/fingerprint"
	"repeats-detector/internal/matching"
	"repeats-detector/internal/testaudio"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL required")
	}
	pool, e := pgxpool.New(context.Background(), url)
	if e != nil {
		t.Fatal(e)
	}
	schema := fmt.Sprintf("test_%d", time.Now().UnixNano())
	if _, e = pool.Exec(context.Background(), "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	cfg := pool.Config()
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	scoped, e := pgxpool.NewWithConfig(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	admin := pool
	t.Cleanup(func() {
		scoped.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	pool = scoped
	s := &Store{Pool: pool, Window: time.Hour, MaxHits: 200000, Match: matching.Default()}
	if e = s.Migrate(context.Background()); e != nil {
		t.Fatal(e)
	}
	return s
}
func TestConcurrentDecisionsAndOutbox(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	scope := fmt.Sprintf("test-%d", time.Now().UnixNano())
	t.Cleanup(func() { _, _ = s.Pool.Exec(ctx, "DELETE FROM rd_tasks WHERE scope_id=$1", scope) })
	p, e := fingerprint.New().Extract(ctx, testaudio.Signal(32, 5))
	if e != nil {
		t.Fatal(e)
	}
	tasks := []domain.Task{{SchemaVersion: 1, TaskID: "one", TrackID: "one", S3URL: "s3://test/one", ScopeID: scope}, {SchemaVersion: 1, TaskID: "two", TrackID: "two", S3URL: "s3://test/two", ScopeID: scope}}
	results := make([]domain.Result, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range tasks {
		wg.Add(1)
		go func(i int) { defer wg.Done(); results[i], errs[i] = s.Decide(ctx, tasks[i], p, nil) }(i)
	}
	wg.Wait()
	counts := map[string]int{}
	for i, e := range errs {
		if e != nil {
			t.Fatal(e)
		}
		counts[results[i].Status]++
	}
	if counts["unique"] != 1 || counts["duplicate"] != 1 {
		t.Fatal(counts)
	}
	for i, task := range tasks {
		r, e := s.Decide(ctx, task, p, nil)
		if e != nil || r.Status != results[i].Status {
			t.Fatal("replay changed decision", r, e)
		}
	}
	bad := tasks[0]
	bad.S3URL = "s3://test/changed"
	if _, e = s.Decide(ctx, bad, p, nil); !errors.Is(e, domain.ErrConflict) {
		t.Fatal("expected conflict", e)
	}
	failed := errors.New("transport down")
	_, e = s.PublishOne(ctx, func(context.Context, string, []byte, map[string]string) error { return failed })
	if !errors.Is(e, failed) {
		t.Fatal(e)
	}
	for range tasks {
		worked, e := s.PublishOne(ctx, func(_ context.Context, id string, b []byte, h map[string]string) error {
			var r domain.Result
			if e := json.Unmarshal(b, &r); e != nil {
				return e
			}
			if r.EventID != id {
				t.Fatal("event identity changed")
			}
			return nil
		})
		if e != nil || !worked {
			t.Fatal(worked, e)
		}
	}
	for _, r := range results {
		yes, e := s.Delivered(ctx, r.EventID)
		if e != nil || !yes {
			t.Fatal(yes, e)
		}
	}
}
func TestWindowAndPartialIndex(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	scope := fmt.Sprintf("window-%d", time.Now().UnixNano())
	defer s.Pool.Exec(ctx, "DELETE FROM rd_tasks WHERE scope_id=$1", scope)
	ex := fingerprint.New()
	ref := testaudio.Signal(7, 8)
	p, _ := ex.Extract(ctx, ref)
	task := domain.Task{SchemaVersion: 1, TaskID: "a", TrackID: "a", S3URL: "s3://test/a", ScopeID: scope}
	r, e := s.Decide(ctx, task, p, nil)
	if e != nil || r.Status != "unique" {
		t.Fatal(r, e)
	}
	combined := append(testaudio.Crop(ref, 2, 4), testaudio.Signal(98, 5)...)
	q, _ := ex.Extract(ctx, combined)
	task.TaskID = "b"
	task.TrackID = "b"
	r, e = s.Decide(ctx, task, q, nil)
	if e != nil || r.Status != "duplicate" {
		t.Fatal(r, e)
	}
	q, _ = ex.Extract(ctx, testaudio.Signal(98, 5))
	task.TaskID = "c"
	task.TrackID = "c"
	r, e = s.Decide(ctx, task, q, nil)
	if e != nil || r.Status != "duplicate" || r.Match.OriginalTrackID != "b" {
		t.Fatal("new tail lost", r, e)
	}
	_, e = s.Pool.Exec(ctx, "UPDATE rd_prints SET created_at=clock_timestamp()-interval '61 minutes' WHERE scope_id=$1", scope)
	if e != nil {
		t.Fatal(e)
	}
	task.TaskID = "d"
	task.TrackID = "d"
	r, e = s.Decide(ctx, task, p, nil)
	if e != nil || r.Status != "unique" {
		t.Fatal("expired evidence found", r, e)
	}
}

func TestRetentionAndCandidateLimit(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	task := domain.Task{SchemaVersion: 1, TaskID: "retained", TrackID: "recording", S3URL: "s3://test/source", ScopeID: "scope"}
	p, _ := fingerprint.New().Extract(ctx, testaudio.Signal(8, 5))
	if _, e := s.Decide(ctx, task, p, nil); e != nil {
		t.Fatal(e)
	}
	s.MaxHits = 1
	next := task
	next.TaskID = "overflow"
	if _, e := s.Decide(ctx, next, p, nil); e == nil {
		t.Fatal("truncated candidates classified")
	}
	if r, e := s.Existing(ctx, next); e != nil || r != nil {
		t.Fatal("failed search committed a decision", r, e)
	}
	if _, e := s.Pool.Exec(ctx, "UPDATE rd_tasks SET created_at=clock_timestamp()-interval '10 days'"); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Pool.Exec(ctx, "UPDATE rd_prints SET created_at=clock_timestamp()-interval '2 hours'"); e != nil {
		t.Fatal(e)
	}
	if e := s.Cleanup(ctx, 90*time.Minute, 7*24*time.Hour); e != nil {
		t.Fatal(e)
	}
	var n int
	if e := s.Pool.QueryRow(ctx, "SELECT count(*) FROM rd_tasks").Scan(&n); e != nil || n != 1 {
		t.Fatal("pending task lost", n, e)
	}
	if e := s.Pool.QueryRow(ctx, "SELECT count(*) FROM rd_prints").Scan(&n); e != nil || n != 0 {
		t.Fatal("expired index retained", n, e)
	}
	if _, e := s.Pool.Exec(ctx, "UPDATE rd_outbox SET delivered_at=clock_timestamp()"); e != nil {
		t.Fatal(e)
	}
	if e := s.Cleanup(ctx, 90*time.Minute, 7*24*time.Hour); e != nil {
		t.Fatal(e)
	}
	if e := s.Pool.QueryRow(ctx, "SELECT count(*) FROM rd_tasks").Scan(&n); e != nil || n != 0 {
		t.Fatal("delivered task not cleaned", n, e)
	}
}

// A long query must find a copy crossing a search-window boundary, while noisy
// high-frequency hashes are excluded in full rather than truncated to arbitrary rows.
func TestWindowedSearchAndFrequentHashes(t *testing.T) {
	s := testStore(t)
	s.MaxPostings = 32
	ctx := context.Background()
	ps, e := fingerprint.New().Extract(ctx, testaudio.Signal(99, 24))
	if e != nil {
		t.Fatal(e)
	}
	task := func(id string) domain.Task {
		return domain.Task{SchemaVersion: 1, TaskID: id, TrackID: id, ScopeID: "windowed", S3URL: "s3://audio/" + id}
	}
	original, e := s.Decide(ctx, task("original"), ps, nil)
	if e != nil || original.Status != "unique" {
		t.Fatalf("%+v %v", original, e)
	}
	copy, e := s.Decide(ctx, task("copy"), ps, nil)
	if e != nil || copy.Status != "duplicate" || copy.Match.OriginalTrackID != "original" {
		t.Fatalf("%+v %v", copy, e)
	}
	// Exhaust the postings budget for just one artificial hash.
	common := []domain.Print{}
	for i := 0; i < 100; i++ {
		common = append(common, domain.Print{Hash: 888888, Frame: i * 10})
	}
	// Mixed informative prints ensure this task is indexed; the common hash is
	// irrelevant to the real copy and must not cause candidate overflow.
	mixed := append(append([]domain.Print(nil), ps...), common...)
	_, e = s.Decide(ctx, task("common"), mixed, nil)
	if e != nil {
		t.Fatal(e)
	}
	s.MaxHits = 1
	// A query consisting entirely of excluded hashes must fail closed.
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	hits, e := s.candidates(ctx, tx, task("query"), []int64{888888}, []int32{0}, time.Now().Add(time.Second))
	if e == nil || len(hits) != 0 {
		t.Fatalf("frequent hash must fail closed: %d hits, %v", len(hits), e)
	}
}

func TestRuntimeRoleWithoutDDLAndBacklog(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	role := fmt.Sprintf("rd_worker_%d", time.Now().UnixNano())
	ident := pgx.Identifier{role}.Sanitize()
	schemaName := s.Pool.Config().ConnConfig.RuntimeParams["search_path"]
	schemaID := pgx.Identifier{schemaName}.Sanitize()
	if _, e := s.Pool.Exec(ctx, "CREATE ROLE "+ident+" NOLOGIN"); e != nil {
		t.Fatal(e)
	}
	defer func() { _, _ = s.Pool.Exec(ctx, "DROP OWNED BY "+ident); _, _ = s.Pool.Exec(ctx, "DROP ROLE "+ident) }()
	for _, sql := range []string{"GRANT USAGE ON SCHEMA " + schemaID + " TO " + ident,
		"GRANT SELECT ON rd_schema TO " + ident,
		"GRANT SELECT,INSERT,UPDATE,DELETE ON rd_tasks,rd_prints,rd_outbox TO " + ident} {
		if _, e := s.Pool.Exec(ctx, sql); e != nil {
			t.Fatal(e)
		}
	}
	cfg := s.Pool.Config()
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error { _, e := c.Exec(ctx, "SET ROLE "+ident); return e }
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	runtime := *s
	runtime.Pool = pool
	if e = runtime.CheckSchema(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, "CREATE TABLE forbidden_ddl(id int)"); e == nil {
		t.Fatal("runtime role can create tables")
	}
	ps, e := fingerprint.New().Extract(ctx, testaudio.Signal(11, 5))
	if e != nil {
		t.Fatal(e)
	}
	task := domain.Task{SchemaVersion: 1, TaskID: "role-task", TrackID: "role-track", ScopeID: "role-scope", S3URL: "s3://audio/role.wav"}
	if _, e = runtime.Decide(ctx, task, ps, nil); e != nil {
		t.Fatal(e)
	}
	count, age, e := runtime.Backlog(ctx)
	if e != nil || count != 1 || age < 0 {
		t.Fatalf("backlog %d %f %v", count, age, e)
	}
	if _, e = runtime.PublishOne(ctx, func(context.Context, string, []byte, map[string]string) error { return nil }); e != nil {
		t.Fatal(e)
	}
	count, age, e = runtime.Backlog(ctx)
	if e != nil || count != 0 || age != 0 {
		t.Fatalf("delivered backlog %d %f %v", count, age, e)
	}
}

func TestCleanupDrainsMultipleBatches(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	task := domain.Task{SchemaVersion: 1, TaskID: "cleanup-many", TrackID: "cleanup-many", ScopeID: "cleanup-many", S3URL: "s3://audio/test.wav"}
	if _, e := s.Decide(ctx, task, nil, nil); e != nil {
		t.Fatal(e)
	}
	_, e := s.Pool.Exec(ctx, `INSERT INTO rd_prints(scope_id,task_id,track_id,version,hash,frame,created_at)
 SELECT $1,$1,$1,$2,n,0,clock_timestamp()-interval '2 hours' FROM generate_series(1,50001)n`, task.TaskID, fingerprint.Version)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Cleanup(ctx, 90*time.Minute, 7*24*time.Hour); e != nil {
		t.Fatal(e)
	}
	var count int
	if e = s.Pool.QueryRow(ctx, "SELECT count(*) FROM rd_prints").Scan(&count); e != nil || count != 0 {
		t.Fatalf("remaining prints %d: %v", count, e)
	}
}

func TestFrequentHashFallbackRetainsCopy(t *testing.T) {
	s := testStore(t)
	s.MaxPostings = 1
	ctx := context.Background()
	ps, e := fingerprint.New().Extract(ctx, testaudio.Signal(22, 8))
	if e != nil {
		t.Fatal(e)
	}
	task := domain.Task{SchemaVersion: 1, TaskID: "frequent-ref", TrackID: "frequent-ref", ScopeID: "frequent", S3URL: "s3://audio/ref"}
	if _, e = s.Decide(ctx, task, ps, nil); e != nil {
		t.Fatal(e)
	}
	task.TaskID = "frequent-copy"
	task.TrackID = "frequent-copy"
	r, e := s.Decide(ctx, task, ps, nil)
	if e != nil || r.Status != "duplicate" {
		t.Fatalf("copy lost: %+v %v", r, e)
	}
}

func TestFrequentHashFallbackRetainsPartialCopy(t *testing.T) {
	s := testStore(t)
	s.MaxPostings = 32
	ctx := context.Background()
	task := func(id string) domain.Task {
		return domain.Task{SchemaVersion: 1, TaskID: id, TrackID: id, ScopeID: "partial-common", S3URL: "s3://audio/" + id}
	}
	// Two frequent hashes alone confirm a 2.128-second interval. Additional
	// distinct hashes make the reference informative without helping this query.
	var reference []domain.Print
	for f := 0; f <= 132; f += 4 {
		reference = append(reference, domain.Print{Hash: 1, Frame: f}, domain.Print{Hash: 2, Frame: f})
	}
	for i := 0; i < 12; i++ {
		reference = append(reference, domain.Print{Hash: int64(100 + i), Frame: i * 12})
	}
	if r, err := s.Decide(ctx, task("reference"), reference, nil); err != nil || r.Status != "unique" {
		t.Fatalf("reference: %+v %v", r, err)
	}
	var query []domain.Print
	for f := 0; f <= 132; f += 12 {
		query = append(query, domain.Print{Hash: 1, Frame: f}, domain.Print{Hash: 2, Frame: f})
	}
	// The majority of query hashes are new and absent from the index. They
	// must not suppress fallback for the common hashes in the repeated part.
	for i := 0; i < 20; i++ {
		query = append(query, domain.Print{Hash: int64(1000 + i), Frame: 160 + i*10})
	}
	for _, tc := range []struct {
		name     string
		maxHits  int
		overflow bool
	}{
		{"overflow", 10, true},
		{"bounded-copy", 200000, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s.MaxHits = tc.maxHits
			r, err := s.Decide(ctx, task(tc.name), query, nil)
			if tc.overflow {
				if !errors.Is(err, ErrCandidateLimit) {
					t.Fatalf("expected candidate limit, got %+v %v", r, err)
				}
				existing, err := s.Existing(ctx, task(tc.name))
				if err != nil || existing != nil {
					t.Fatalf("overflow committed a classification: %+v %v", existing, err)
				}
			} else if err != nil || r.Status != "duplicate" || r.Match == nil || r.Match.OriginalTrackID != "reference" {
				t.Fatalf("partial copy lost: %+v %v", r, err)
			}
		})
	}
}
