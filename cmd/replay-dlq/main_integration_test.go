//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"os"
	"os/exec"
	"repeats-detector/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestReplayHelper(t *testing.T) {
	if os.Getenv("REPLAY_HELPER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{os.Args[0]}, os.Args[i+1:]...)
			break
		}
	}
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	os.Exit(0)
}
func TestScopedReplayAndUnroutablePreservation(t *testing.T) {
	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		t.Skip("TEST_RABBITMQ_URL required")
	}
	conn, e := amqp.Dial(url)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	ch, e := conn.Channel()
	if e != nil {
		t.Fatal(e)
	}
	defer ch.Close()
	input := fmt.Sprintf("replay_input_%d", time.Now().UnixNano())
	dead := input + "_dead"
	for _, q := range []string{input, dead} {
		if _, e = ch.QueueDeclare(q, true, false, false, false, nil); e != nil {
			t.Fatal(e)
		}
	}
	defer ch.QueueDelete(input, false, false, false)
	defer ch.QueueDelete(dead, false, false, false)
	for _, scope := range []string{"wanted", "unrelated"} {
		task := domain.Task{SchemaVersion: 1, TaskID: scope, TrackID: scope, ScopeID: scope, S3URL: "s3://audio/test.wav"}
		body, _ := json.Marshal(task)
		if e = ch.PublishWithContext(context.Background(), "", dead, true, false, amqp.Publishing{Body: body, Headers: amqp.Table{"x-delivery-count": int64(5), "x-acquired-count": int64(5), "traceparent": "preserve-this"}}); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = ch.QueueInspect(dead); e != nil {
		t.Fatal(e)
	}
	invoke := func(scope, target string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReplayHelper$", "--", "-scope", scope, "-input", target, "-dlq", dead)
		cmd.Env = append(os.Environ(), "REPLAY_HELPER=1", "RABBITMQ_URL="+url)
		return cmd.CombinedOutput()
	}
	out, e := invoke("wanted", input)
	if e != nil {
		t.Fatalf("%s: %v", out, e)
	}
	d, ok, e := ch.Get(input, true)
	if e != nil || !ok {
		t.Fatalf("missing replay: %v", e)
	}
	var task domain.Task
	if e = json.Unmarshal(d.Body, &task); e != nil || task.ScopeID != "wanted" {
		t.Fatalf("wrong replay: %s %v", d.Body, e)
	}
	if _, ok = d.Headers["x-delivery-count"]; ok {
		t.Fatal("retry count followed replay")
	}
	if d.Headers["traceparent"] != "preserve-this" {
		t.Fatal("trace context was lost")
	}
	info, e := ch.QueueInspect(dead)
	if e != nil || info.Messages != 1 {
		t.Fatalf("unrelated message lost: %+v %v", info, e)
	}
	out, e = invoke("unrelated", input+"_missing")
	if e == nil || !strings.Contains(string(out), "unroutable") {
		t.Fatalf("missing route: %s %v", out, e)
	}
	info, e = ch.QueueInspect(dead)
	if e != nil || info.Messages != 1 {
		t.Fatalf("unconfirmed source was acknowledged: %+v %v", info, e)
	}
}
