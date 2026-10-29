// replay-dlq moves only a named scope's dead letters back to the input queue.
// Publish-confirm precedes source Ack; a crash can replay the same task twice,
// which is safe only within the service's task idempotency retention window.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"net"
	"os"
	"repeats-detector/internal/domain"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	scope := flag.String("scope", "", "required exact scope_id; unrelated messages remain queued")
	input := flag.String("input", "audio.tasks", "input queue")
	dlq := flag.String("dlq", "audio.tasks.dead", "dead-letter queue")
	limit := flag.Int("limit", 1000, "bounded inspection count, 1..1000")
	flag.Parse()
	if *scope == "" || *input == *dlq || *limit < 1 || *limit > 1000 {
		return fmt.Errorf("invalid arguments")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var raw net.Conn
	conn, e := amqp.DialConfig(os.Getenv("RABBITMQ_URL"), amqp.Config{Heartbeat: 10 * time.Second, Dial: func(network, addr string) (net.Conn, error) {
		var err error
		raw, err = (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, addr)
		if err == nil {
			err = raw.SetDeadline(time.Now().Add(10 * time.Second))
		}
		return raw, err
	}})
	if e != nil {
		return e
	}
	defer conn.Close()
	// Bound all subsequent management and publication writes as well as reads.
	if e = raw.SetDeadline(time.Now().Add(2 * time.Minute)); e != nil {
		return e
	}
	ch, e := conn.Channel()
	if e != nil {
		return e
	}
	defer ch.Close()
	info, e := ch.QueueInspect(*dlq)
	if e != nil {
		return e
	}
	if e = ch.Confirm(false); e != nil {
		return e
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))
	held := []amqp.Delivery{}
	defer func() {
		for _, d := range held {
			_ = d.Nack(false, true)
		}
	}()
	moved := []string{}
	inspected := 0
	for i := 0; i < min(*limit, info.Messages); i++ {
		d, ok, e := ch.Get(*dlq, false)
		if e != nil {
			return e
		}
		if !ok {
			break
		}
		inspected++
		var t domain.Task
		if json.Unmarshal(d.Body, &t) != nil || t.Validate() != nil || t.ScopeID != *scope {
			held = append(held, d)
			continue
		}
		headers := amqp.Table{}
		// Broker-owned delivery/death counters must not follow a deliberate replay.
		for _, k := range []string{"traceparent", "tracestate"} {
			if v, ok := d.Headers[k].(string); ok {
				headers[k] = v
			}
		}
		if e = ch.PublishWithContext(ctx, "", *input, true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "application/json", MessageId: t.TaskID, Body: d.Body, Headers: headers}); e != nil {
			return e
		}
		select {
		case c, ok := <-confirms:
			if !ok || !c.Ack {
				return fmt.Errorf("replay not confirmed")
			}
			select {
			case <-returns:
				return fmt.Errorf("replay unroutable; source remains in DLQ")
			default:
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		if e = d.Ack(false); e != nil {
			return e
		}
		moved = append(moved, t.TaskID)
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"scope": *scope, "inspected": inspected, "replayed": moved, "unrelated_retained": len(held)})
}
