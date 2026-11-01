package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"

	"repeats-detector/internal/config"
	"repeats-detector/internal/domain"
	"repeats-detector/internal/observability"
	"repeats-detector/internal/usecase"
)

func dial(url string, timeout, heartbeat time.Duration) (*amqp.Connection, error) {
	return amqp.DialConfig(url, amqp.Config{Heartbeat: heartbeat, Dial: func(network, addr string) (net.Conn, error) {
		c, e := net.DialTimeout(network, addr, timeout)
		if e != nil {
			return nil, e
		}
		deadline := time.Now().Add(timeout)
		if e = c.SetDeadline(deadline); e != nil {
			_ = c.Close()
			return nil, e
		}
		return c, nil
	}})
}

// Publisher reuses a confirmed AMQP channel and reconnects after transport errors.
// Calls are serialized so a confirmation always belongs to the current event.
// Socket deadlines and context cancellation bound writes as well as confirmation waits.
type Publisher struct {
	mu       sync.Mutex
	config   config.Config
	conn     *amqp.Connection
	raw      net.Conn
	ch       *amqp.Channel
	confirms <-chan amqp.Confirmation
	returns  <-chan amqp.Return
}

func NewPublisher(c config.Config) *Publisher { return &Publisher{config: c} }
func (p *Publisher) reset() {
	if p.raw != nil {
		_ = p.raw.Close()
	}
	p.conn, p.raw, p.ch = nil, nil, nil
}
func (p *Publisher) Close() { p.mu.Lock(); defer p.mu.Unlock(); p.reset() }
func (p *Publisher) Publish(ctx context.Context, id string, body []byte, headers map[string]string) (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(headers))
	ctx, span := otel.Tracer("repeats-detector").Start(ctx, "Result_Publish")
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, p.config.PublishTimeout)
	defer cancel()
	if err = ctx.Err(); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	if p.conn == nil || p.conn.IsClosed() {
		p.reset()
		p.conn, err = amqp.DialConfig(p.config.RabbitURL, amqp.Config{Heartbeat: p.config.RabbitHeartbeat, Dial: func(network, addr string) (net.Conn, error) {
			var e error
			p.raw, e = (&net.Dialer{}).DialContext(ctx, network, addr)
			if e == nil {
				e = p.raw.SetDeadline(deadline)
			}
			return p.raw, e
		}})
		if err != nil {
			p.reset()
			span.RecordError(err)
			return err
		}

	}
	raw := p.raw
	if err = raw.SetDeadline(deadline); err != nil {
		p.reset()
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer func() {
		stop()
		if err != nil {
			span.RecordError(err)
			p.reset()
		} else {
			_ = raw.SetDeadline(time.Time{})
		}
	}()
	if p.ch == nil {
		p.ch, err = p.conn.Channel()
		if err == nil {
			_, err = p.ch.QueueDeclare(p.config.OutputQueue, true, false, false, false, nil)
		}
		if err == nil {
			err = p.ch.Confirm(false)
		}
		if err != nil {
			p.reset()
			span.RecordError(err)
			return err
		}
		p.confirms = p.ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		p.returns = p.ch.NotifyReturn(make(chan amqp.Return, 1))
	}

	table := amqp.Table{}
	for k, v := range headers {
		table[k] = v
	}
	if err = p.ch.PublishWithContext(ctx, "", p.config.OutputQueue, true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "application/json", MessageId: id, Timestamp: time.Now(), Body: body, Headers: table}); err != nil {
		return err
	}
	select {
	case result, ok := <-p.confirms:
		if !ok || !result.Ack {
			return fmt.Errorf("publication not confirmed")
		}
		select {
		case <-p.returns:
			return fmt.Errorf("unroutable result")
		default:
			return nil
		}
	case <-p.returns:
		return fmt.Errorf("unroutable result")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Publish provides a one-shot adapter for callers that do not own a lifecycle.
// Long-running workers should use NewPublisher and defer Close.
func Publish(c config.Config) func(context.Context, string, []byte, map[string]string) error {
	return func(ctx context.Context, id string, body []byte, headers map[string]string) error {
		p := NewPublisher(c)
		defer p.Close()
		return p.Publish(ctx, id, body, headers)
	}
}

type Consumer struct {
	Config    config.Config
	Processor *usecase.Processor
	Log       *slog.Logger
	Metrics   *observability.Metrics
	Ready     atomic.Bool
}

func (w *Consumer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if e := w.session(ctx); e != nil && ctx.Err() == nil {
			w.Log.ErrorContext(ctx, "RabbitMQ session ended", "error", e)
		}
		if !pause(ctx, w.Config.RabbitReconnectDelay) {
			break
		}
	}
	return nil
}
func (w *Consumer) session(ctx context.Context) error {
	conn, e := dial(w.Config.RabbitURL, w.Config.RabbitConnectTimeout, w.Config.RabbitHeartbeat)
	if e != nil {
		return e
	}
	defer conn.Close()
	// amqp091 resets the handshake deadline after connection establishment.
	ch, e := conn.Channel()
	if e != nil {
		return e
	}
	if _, e = ch.QueueDeclare(w.Config.DLQueue, true, false, false, false, nil); e != nil {
		return e
	}
	if _, e = ch.QueueDeclare(w.Config.OutputQueue, true, false, false, false, nil); e != nil {
		return e
	}
	args := amqp.Table{"x-dead-letter-exchange": "", "x-dead-letter-routing-key": w.Config.DLQueue, "x-queue-type": "quorum", "x-delivery-limit": int32(w.Config.TaskDeliveryLimit)}
	if _, e = ch.QueueDeclare(w.Config.InputQueue, true, false, false, false, args); e != nil {
		return e
	}
	if e = ch.Qos(w.Config.Workers, 0, false); e != nil {
		return e
	}
	deliveries, e := ch.Consume(w.Config.InputQueue, "", false, false, false, false, nil)
	if e != nil {
		return e
	}
	w.Ready.Store(true)
	defer w.Ready.Store(false)
	processCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < w.Config.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				select {
				case <-ctx.Done():
					return
				case d, ok := <-deliveries:
					if !ok {
						return
					}
					w.handle(processCtx, d)
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return fmt.Errorf("delivery stream closed")
	case <-ctx.Done():
	}
	timer := time.NewTimer(w.Config.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		cancel()
		_ = conn.Close()
		<-done
		return ctx.Err()
	}
}
func (w *Consumer) handle(parent context.Context, d amqp.Delivery) {
	w.Metrics.Inflight.Add(parent, 1)
	defer w.Metrics.Inflight.Add(parent, -1)
	start := time.Now()
	headers := propagation.MapCarrier{}
	for k, v := range d.Headers {
		if s, ok := v.(string); ok {
			headers[k] = s
		}
	}
	ctx := otel.GetTextMapPropagator().Extract(parent, headers)
	ctx, cancel := context.WithTimeout(ctx, w.Config.TaskTimeout)
	defer cancel()
	ctx, span := otel.Tracer("repeats-detector").Start(ctx, "Process_RabbitMQ_Message")
	defer span.End()
	status := "error"
	defer func() { w.Metrics.Record(ctx, status, time.Since(start)) }()
	var t domain.Task
	e := error(nil)
	if len(d.Body) > 16384 {
		e = fmt.Errorf("%w: message too large", domain.ErrInvalid)
	} else if err := json.Unmarshal(d.Body, &t); err != nil {
		e = fmt.Errorf("%w: invalid JSON", domain.ErrInvalid)
	}
	if e == nil {
		e = t.Validate()
	}
	span.SetAttributes(attribute.String("audio.track_id", t.TrackID), attribute.String("task.id", t.TaskID))
	if e == nil {
		var r domain.Result
		r, e = w.Processor.Process(ctx, t)
		if e == nil {
			e = w.Processor.WaitDelivered(ctx, r.EventID)
			if e == nil {
				status = r.Status
				e = d.Ack(false)
			}
		}
	}
	if e != nil {
		span.RecordError(e)
		span.SetStatus(codes.Error, "processing failed")
		permanent := errors.Is(e, domain.ErrInvalid) || errors.Is(e, domain.ErrConflict)
		exhausted := deliveryAttempts(d.Headers) >= int64(w.Config.TaskDeliveryLimit)
		w.Log.ErrorContext(ctx, "audio processing failed", "task_id", t.TaskID, "permanent", permanent, "error", e)
		if !permanent && !exhausted {
			w.Metrics.Retries.Add(ctx, 1)
			pause(ctx, w.Config.TaskRetryDelay)
		} else {
			status = "rejected"
			if exhausted {
				status = "retry_exhausted"
			}
		}
		if err := d.Nack(false, !permanent && !exhausted); err != nil {
			w.Log.ErrorContext(ctx, "delivery nack failed", "error", err)
		}
	}
}
func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// RabbitMQ 4.3 counts Nack returns in x-acquired-count rather than delivery-count.
// Check both so bounded retries work on older and newer quorum queues.
func deliveryAttempts(headers amqp.Table) int64 {
	var n int64
	for _, key := range []string{"x-acquired-count", "x-delivery-count"} {
		var v int64
		switch x := headers[key].(type) {
		case int64:
			v = x
		case int32:
			v = int64(x)
		case int:
			v = int64(x)
		}
		if v > n {
			n = v
		}
	}
	return n
}
