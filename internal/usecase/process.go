package usecase

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"

	"repeats-detector/internal/audio"
	"repeats-detector/internal/domain"
	"repeats-detector/internal/observability"
)

type AudioSource interface {
	Load(context.Context, string) ([]byte, error)
}
type Fingerprinter interface {
	Extract(context.Context, audio.PCM) ([]domain.Print, error)
}
type Repository interface {
	Existing(context.Context, domain.Task) (*domain.Result, error)
	Decide(context.Context, domain.Task, []domain.Print, map[string]string) (domain.Result, error)
	Delivered(context.Context, string) (bool, error)
}
type Processor struct {
	Source    AudioSource
	Extractor Fingerprinter
	Store     Repository
	MaxPrints int
	Metrics   *observability.Metrics
}

func (p *Processor) Process(ctx context.Context, t domain.Task) (domain.Result, error) {
	if e := t.Validate(); e != nil {
		return domain.Result{}, e
	}
	if existing, e := p.Store.Existing(ctx, t); e != nil {
		return domain.Result{}, e
	} else if existing != nil {
		return *existing, nil
	}
	tracer := otel.Tracer("repeats-detector")
	stage := func(name string, f func(context.Context) error) error {
		c, span := tracer.Start(ctx, name)
		defer span.End()
		start := time.Now()
		e := f(c)
		p.Metrics.StageTime(c, name, start)
		if e != nil {
			span.RecordError(e)
		}
		return e
	}
	var data []byte
	if e := stage("S3_Download", func(c context.Context) error { var e error; data, e = p.Source.Load(c, t.S3URL); return e }); e != nil {
		return domain.Result{}, e
	}
	var pcm audio.PCM
	if e := stage("Audio_Prepare", func(c context.Context) error { var e error; pcm, e = audio.Parse(data); return e }); e != nil {
		return domain.Result{}, e
	}
	var prints []domain.Print
	if e := stage("Fingerprint_Extract", func(c context.Context) error { var e error; prints, e = p.Extractor.Extract(c, pcm); return e }); e != nil {
		return domain.Result{}, e
	}
	if len(prints) > p.MaxPrints {
		return domain.Result{}, fmt.Errorf("%w: fingerprint limit exceeded", domain.ErrInvalid)
	}
	attributes := attribute.Int("audio.fingerprint_count", len(prints))
	c, span := tracer.Start(ctx, "Search_Decide_Index")
	span.SetAttributes(attributes)
	defer span.End()
	headers := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, headers)
	r, e := p.Store.Decide(c, t, prints, headers)
	if e != nil {
		span.RecordError(e)
	}
	return r, e
}
func (p *Processor) WaitDelivered(ctx context.Context, id string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		yes, e := p.Store.Delivered(ctx, id)
		if e != nil {
			return e
		}
		if yes {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
