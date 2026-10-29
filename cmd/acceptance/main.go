// acceptance exercises actual S3 -> RabbitMQ -> service -> PostgreSQL outbox.
// It uses a new scope and object prefix on each run and never purges shared queues.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"errors"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"repeats-detector/internal/domain"
)

type file struct {
	Path  string `json:"path"`
	Track string `json:"track_id"`
}
type query struct {
	Path          string `json:"path"`
	Name          string `json:"name"`
	ExpectedTrack string `json:"expected_track"`
	Seconds       int    `json:"seconds"`
}
type manifest struct {
	Gallery []file  `json:"gallery"`
	Queries []query `json:"queries"`
}
type measured struct {
	TaskID       string        `json:"task_id"`
	Name         string        `json:"name"`
	Milliseconds float64       `json:"milliseconds"`
	Result       domain.Result `json:"result"`
}

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	manifestPath := flag.String("manifest", "", "long corpus manifest")
	endpoint := flag.String("s3-endpoint", "http://localhost:9002", "MinIO endpoint")
	bucket := flag.String("bucket", "audio", "test bucket")
	queue := flag.String("queue", "audio.tasks", "input queue")
	limit := flag.Int("queries", 30, "positive queries (>=5s), 0 means all")
	concurrency := flag.Int("concurrency", 2, "maximum outstanding tasks")
	delay := flag.Duration("query-delay", 0, "optional pacing between query batches for fault tests")
	output := flag.String("output", "", "JSON report file")
	flag.Parse()
	if *manifestPath == "" || *concurrency < 1 || *limit < 0 {
		return fmt.Errorf("invalid arguments")
	}
	raw, e := os.ReadFile(*manifestPath)
	if e != nil {
		return e
	}
	var m manifest
	if e = json.Unmarshal(raw, &m); e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	db, e := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if e != nil {
		return e
	}
	defer db.Close()
	ac, e := awsconfig.LoadDefaultConfig(ctx)
	if e != nil {
		return e
	}
	client := s3.NewFromConfig(ac, func(o *s3.Options) { o.BaseEndpoint = endpoint; o.UsePathStyle = true })
	if _, e = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}); e != nil {
		var api smithy.APIError
		if !errors.As(e, &api) || (api.ErrorCode() != "BucketAlreadyOwnedByYou" && api.ErrorCode() != "BucketAlreadyExists") {
			return e
		}
	}
	b := make([]byte, 8)
	if _, e = rand.Read(b); e != nil {
		return e
	}
	scope := "acceptance-" + hex.EncodeToString(b)
	conn, e := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if e != nil {
		return e
	}
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	ch, e := conn.Channel()
	if e != nil {
		return e
	}
	defer ch.Close()
	if e = ch.Confirm(false); e != nil {
		return e
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))
	base := filepath.Dir(*manifestPath)
	reports := []measured{}
	upload := func(path, key string) error {
		if !filepath.IsAbs(path) {
			path = filepath.Join(base, path)
		}
		f, e := os.Open(path)
		if e != nil {
			return e
		}
		defer f.Close()
		_, e = client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: &key, Body: f})
		return e
	}
	reconnect := func() error {
		if conn != nil {
			_ = conn.Close()
		}
		var err error
		conn, err = amqp.Dial(os.Getenv("RABBITMQ_URL"))
		if err != nil {
			return err
		}
		ch, err = conn.Channel()
		if err != nil {
			return err
		}
		if err = ch.Confirm(false); err != nil {
			return err
		}
		confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		returns = ch.NotifyReturn(make(chan amqp.Return, 1))
		return nil
	}
	send := func(id, track, key string) error {
		task := domain.Task{SchemaVersion: 1, TaskID: id, TrackID: track, ScopeID: scope, S3URL: "s3://" + *bucket + "/" + key}
		data, _ := json.Marshal(task)
		deadline := time.Now().Add(2 * time.Minute)
	sendLoop:
		for time.Now().Before(deadline) && ctx.Err() == nil {
			if conn == nil || conn.IsClosed() {
				if err := reconnect(); err != nil {
					time.Sleep(time.Second)
					continue
				}
			}
			err := ch.PublishWithContext(ctx, "", *queue, true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "application/json", MessageId: id, Body: data})
			if err == nil {
				select {
				case c, ok := <-confirms:
					if ok && c.Ack {
						select {
						case <-returns:
							time.Sleep(time.Second)
							continue sendLoop
						default:
							return nil
						}
					}
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(10 * time.Second):
				}
			}
			if err := reconnect(); err != nil {
				time.Sleep(time.Second)
			}
		}
		return fmt.Errorf("input publication recovery timed out: %s", id)
	}
	wait := func(id, name string, started time.Time) (measured, error) {
		var data []byte
		for {
			e := db.QueryRow(ctx, `SELECT t.result FROM rd_tasks t JOIN rd_outbox o USING(scope_id,task_id)
    WHERE t.scope_id=$1 AND t.task_id=$2 AND o.delivered_at IS NOT NULL`, scope, id).Scan(&data)
			if e == nil {
				var r domain.Result
				if e = json.Unmarshal(data, &r); e != nil {
					return measured{}, e
				}
				return measured{id, name, float64(time.Since(started).Microseconds()) / 1000, r}, nil
			}
			if ctx.Err() != nil {
				return measured{}, ctx.Err()
			}
			if !errors.Is(e, pgx.ErrNoRows) {
				fmt.Fprintln(os.Stderr, "waiting for database recovery:", e)
			}
			if time.Since(started) > 5*time.Minute {
				return measured{}, fmt.Errorf("task timed out: %s", id)
			}
			select {
			case <-ctx.Done():
				return measured{}, ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	for i, r := range m.Gallery {
		key := scope + "/gallery/" + r.Track + ".wav"
		if e = upload(r.Path, key); e != nil {
			return e
		}
		id := fmt.Sprintf("%s-ref-%d", scope, i)
		started := time.Now()
		if e = send(id, r.Track, key); e != nil {
			return e
		}
		result, e := wait(id, r.Track, started)
		if e != nil {
			return e
		}
		reports = append(reports, result)
		fmt.Fprintf(os.Stderr, "gallery %d/%d %s: %s %.0fms\n", i+1, len(m.Gallery), r.Track, result.Result.Status, result.Milliseconds)
	}
	type pending struct {
		id, name string
		started  time.Time
	}
	pendingTasks := []pending{}
	queries := 0
	failures := 0
	timings := []float64{}
	drain := func() error {
		for _, p := range pendingTasks {
			r, e := wait(p.id, p.name, p.started)
			if e != nil {
				return e
			}
			reports = append(reports, r)
			timings = append(timings, r.Milliseconds)
			if r.Result.Status != "duplicate" {
				failures++
			}
		}
		pendingTasks = nil
		return nil
	}
	loadStart := time.Now()
	for _, q := range m.Queries {
		if q.ExpectedTrack == "" || q.Seconds < 5 {
			continue
		}
		if *limit > 0 && queries >= *limit {
			break
		}
		key := scope + "/queries/" + q.Name + ".wav"
		if e = upload(q.Path, key); e != nil {
			return e
		}
		id := fmt.Sprintf("%s-query-%d", scope, queries)
		started := time.Now()
		if e = send(id, q.Name, key); e != nil {
			return e
		}
		pendingTasks = append(pendingTasks, pending{id, q.Name, started})
		queries++
		if len(pendingTasks) >= *concurrency {
			if e = drain(); e != nil {
				return e
			}
			fmt.Fprintf(os.Stderr, "queries %d, failures %d\n", queries, failures)
			if *delay > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(*delay):
				}
			}
		}
	}
	if e = drain(); e != nil {
		return e
	}
	sort.Float64s(timings)
	pct := func(p float64) float64 {
		if len(timings) == 0 {
			return 0
		}
		return timings[int(float64(len(timings)-1)*p)]
	}
	report := map[string]any{"scope": scope, "s3_endpoint": *endpoint, "gallery": len(m.Gallery), "queries": queries, "failures": failures, "concurrency": *concurrency, "elapsed_seconds": time.Since(loadStart).Seconds(), "p50_ms": pct(.5), "p95_ms": pct(.95), "p99_ms": pct(.99), "results": reports, "limitations": "Only known positive queries >=5s; source and query objects are retained; PostgreSQL result plus delivered outbox checked, output consumer not included."}
	out := os.Stdout
	if *output != "" {
		out, e = os.Create(*output)
		if e != nil {
			return e
		}
		defer out.Close()
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if e = enc.Encode(report); e != nil {
		return e
	}
	if failures > 0 {
		return fmt.Errorf("%d positive queries were not classified as duplicates", failures)
	}
	return nil
}
