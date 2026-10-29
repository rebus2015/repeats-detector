//go:build integration

package worker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	"repeats-detector/internal/config"
	"repeats-detector/internal/domain"
	"repeats-detector/internal/fingerprint"
	"repeats-detector/internal/matching"
	"repeats-detector/internal/observability"
	"repeats-detector/internal/source"
	"repeats-detector/internal/store"
	"repeats-detector/internal/testaudio"
	"repeats-detector/internal/usecase"
)

func TestPipeline(t *testing.T) {
	db, rabbit := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_RABBITMQ_URL")
	if db == "" || rabbit == "" {
		t.Skip("TEST_DATABASE_URL and TEST_RABBITMQ_URL required")
	}
	t.Setenv("OTEL_SDK_DISABLED", "true")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, e := pgxpool.New(ctx, db)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	schema := fmt.Sprintf("pipeline_%d", time.Now().UnixNano())
	_, e = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	cfg := admin.Config()
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	repo := &store.Store{Pool: pool, Window: time.Hour, MaxHits: 200000, Match: matching.Default()}
	if e = repo.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	ref := testaudio.Signal(91, 8)
	objects := map[string][]byte{"/audio/original.wav": testaudio.WAV(ref), "/audio/crop.wav": testaudio.WAV(testaudio.Noise(testaudio.Crop(ref, 1.357, 5), 12))}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, ok := objects[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "audio/wav")
		_, _ = w.Write(b)
	}))
	defer server.Close()
	endpoint := server.URL
	client := s3.New(s3.Options{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), BaseEndpoint: &endpoint, UsePathStyle: true, RetryMaxAttempts: 1})
	metrics, shutdown, e := observability.Setup(ctx, "http://localhost:4317")
	if e != nil {
		t.Fatal(e)
	}
	defer shutdown(context.Background())
	p := &usecase.Processor{Source: &source.S3{Client: client, MaxBytes: 1000000}, Extractor: fingerprint.New(), Store: repo, MaxPrints: 100000, Metrics: metrics}
	c := config.Config{RabbitURL: rabbit, InputQueue: schema + ".in", OutputQueue: schema + ".out", DLQueue: schema + ".dlq", Workers: 2, TaskTimeout: 20 * time.Second, PublishTimeout: 5 * time.Second, ShutdownTimeout: 5 * time.Second}
	conn, e := dial(rabbit, 5*time.Second)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	ch, e := conn.Channel()
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		for _, q := range []string{c.InputQueue, c.OutputQueue, c.DLQueue} {
			_, _ = ch.QueueDelete(q, false, false, false)
		}
	}()
	consumer := Consumer{Config: c, Processor: p, Log: slog.New(slog.NewJSONHandler(io.Discard, nil)), Metrics: metrics}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()
	defer func() {
		stop()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("consumer shutdown stuck")
		}
	}()
	for !consumer.Ready.Load() {
		select {
		case <-ctx.Done():
			t.Fatal("consumer not ready")
		case <-time.After(20 * time.Millisecond):
		}
	}
	outCtx, outCancel := context.WithCancel(ctx)
	defer outCancel()
	outDone := make(chan struct{})
	go func() {
		defer close(outDone)
		for outCtx.Err() == nil {
			worked, err := repo.PublishOne(outCtx, Publish(c))
			if err != nil && outCtx.Err() == nil {
				t.Log("outbox retry", err)
			}
			if !worked {
				pause(outCtx, 20*time.Millisecond)
			}
		}
	}()
	defer func() { outCancel(); <-outDone }()
	send := func(body []byte) {
		t.Helper()
		if e := ch.PublishWithContext(ctx, "", c.InputQueue, true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "application/json", Body: body}); e != nil {
			t.Fatal(e)
		}
	}
	get := func(queue string) amqp.Delivery {
		t.Helper()
		for {
			d, ok, e := ch.Get(queue, false)
			if e != nil {
				t.Fatal(e)
			}
			if ok {
				if e = d.Ack(false); e != nil {
					t.Fatal(e)
				}
				return d
			}
			select {
			case <-ctx.Done():
				t.Fatal("waiting for", queue)
				return amqp.Delivery{}
			case <-time.After(30 * time.Millisecond):
			}
		}
	}
	for i, key := range []string{"original.wav", "crop.wav"} {
		task := domain.Task{SchemaVersion: 1, TaskID: fmt.Sprint(i), TrackID: key, ScopeID: schema, S3URL: "s3://audio/" + key}
		b, _ := json.Marshal(task)
		send(b)
		d := get(c.OutputQueue)
		var r domain.Result
		if e = json.Unmarshal(d.Body, &r); e != nil {
			t.Fatal(e)
		}
		want := "unique"
		if i == 1 {
			want = "duplicate"
		}
		if r.Status != want || d.MessageId != r.EventID {
			t.Fatal(r, d.MessageId)
		}
		if i == 1 && (r.Match == nil || r.Match.OriginalTrackID != "original.wav") {
			t.Fatal(r)
		}
		// Re-delivery uses the committed result even if the source is unavailable.
		task.Validate()
		saved, e := p.Process(ctx, task)
		if e != nil || saved.Status != want {
			t.Fatal("replay failed", saved, e)
		}
	}
	// A missing object retries and must eventually land in DLQ, including on
	// RabbitMQ versions where basic.nack does not increment delivery-count.
	missing := domain.Task{SchemaVersion: 1, TaskID: "missing", TrackID: "missing", ScopeID: schema, S3URL: "s3://audio/missing.wav"}
	missingBody, _ := json.Marshal(missing)
	send(missingBody)
	failed := get(c.DLQueue)
	if string(failed.Body) != string(missingBody) {
		t.Fatal("wrong exhausted message")
	}
	send([]byte("{invalid"))
	_ = get(c.DLQueue)
	var count int
	if e = pool.QueryRow(ctx, "SELECT count(*) FROM rd_outbox WHERE delivered_at IS NOT NULL").Scan(&count); e != nil || count != 2 {
		t.Fatal(count, e)
	}
}

func TestPersistentPublisherReconnect(t *testing.T) {
	rabbit := os.Getenv("TEST_RABBITMQ_URL")
	if rabbit == "" {
		t.Skip("TEST_RABBITMQ_URL required")
	}
	queue := fmt.Sprintf("publisher_test_%d", time.Now().UnixNano())
	p := NewPublisher(config.Config{RabbitURL: rabbit, OutputQueue: queue, PublishTimeout: 3 * time.Second})
	defer p.Close()
	defer func() {
		c, e := amqp.Dial(rabbit)
		if e != nil {
			return
		}
		defer c.Close()
		ch, e := c.Channel()
		if e != nil {
			return
		}
		defer ch.Close()
		_, _ = ch.QueueDelete(queue, false, false, false)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if e := p.Publish(ctx, "one", []byte(`{}`), nil); e != nil {
		t.Fatal(e)
	}
	first := p.conn
	if e := p.Publish(ctx, "two", []byte(`{}`), nil); e != nil {
		t.Fatal(e)
	}
	if p.conn != first {
		t.Fatal("connection was not reused")
	}
	if e := p.conn.Close(); e != nil {
		t.Fatal(e)
	}
	if e := p.Publish(ctx, "three", []byte(`{}`), nil); e != nil {
		t.Fatal(e)
	}
	if p.conn == first {
		t.Fatal("closed connection reused")
	}
	expired, stop := context.WithCancel(ctx)
	stop()
	if e := p.Publish(expired, "four", []byte(`{}`), nil); e == nil {
		t.Fatal("cancelled publish succeeded")
	}
}

// The proxy forwards the AMQP handshake but blackholes Channel.Open. This
// catches timeouts installed too late, after synchronous channel setup RPCs.
func TestPublisherBoundsChannelSetup(t *testing.T) {
	rabbit := os.Getenv("TEST_RABBITMQ_URL")
	if rabbit == "" {
		t.Skip("TEST_RABBITMQ_URL required")
	}
	u, e := url.Parse(rabbit)
	if e != nil {
		t.Fatal(e)
	}
	if u.Scheme != "amqp" {
		t.Skip("test proxy expects plain local AMQP")
	}
	upstreamAddr := u.Host
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observed := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		client, e := listener.Accept()
		if e != nil {
			return
		}
		defer client.Close()
		upstream, e := net.DialTimeout("tcp", upstreamAddr, time.Second)
		if e != nil {
			return
		}
		defer upstream.Close()
		copyDone := make(chan struct{})
		go func() { defer close(copyDone); _, _ = io.Copy(client, upstream) }()
		defer func() { _ = upstream.Close(); _ = client.Close(); <-copyDone }()
		protocol := make([]byte, 8)
		if _, e = io.ReadFull(client, protocol); e != nil {
			return
		}
		if _, e = upstream.Write(protocol); e != nil {
			return
		}
		for {
			header := make([]byte, 7)
			if _, e = io.ReadFull(client, header); e != nil {
				return
			}
			size := binary.BigEndian.Uint32(header[3:])
			if size > 1024*1024 {
				return
			}
			body := make([]byte, int(size)+1)
			if _, e = io.ReadFull(client, body); e != nil {
				return
			}
			if header[1] != 0 || header[2] != 0 {
				close(observed)
				<-ctx.Done()
				return
			}
			if _, e = upstream.Write(append(header, body...)); e != nil {
				return
			}
		}
	}()
	u.Host = listener.Addr().String()
	publisher := NewPublisher(config.Config{RabbitURL: u.String(), OutputQueue: "unused_blackhole_test", PublishTimeout: 500 * time.Millisecond})
	defer publisher.Close()
	start := time.Now()
	e = publisher.Publish(ctx, "bounded", []byte(`{}`), nil)
	if e == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("setup was not bounded: %v, %v", e, time.Since(start))
	}
	select {
	case <-observed:
	default:
		t.Fatal("proxy did not observe Channel.Open")
	}
	cancel()
	<-done
}
