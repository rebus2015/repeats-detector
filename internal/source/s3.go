package source

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"repeats-detector/internal/domain"
)

type S3 struct {
	Client   *s3.Client
	MaxBytes int
}

func (s *S3) Load(ctx context.Context, location string) ([]byte, error) {
	u, e := url.Parse(location)
	if e != nil || u.Scheme != "s3" || u.Host == "" || u.Path == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, fmt.Errorf("%w: use s3://bucket/key", domain.ErrInvalid)
	}
	key := strings.TrimPrefix(u.Path, "/")
	if key == "" {
		return nil, fmt.Errorf("%w: empty S3 key", domain.ErrInvalid)
	}
	out, e := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(u.Host), Key: aws.String(key)})
	if e != nil {
		return nil, fmt.Errorf("get S3 object: %w", e)
	}
	defer out.Body.Close()
	if out.ContentLength != nil && *out.ContentLength > int64(s.MaxBytes) {
		return nil, fmt.Errorf("%w: WAV exceeds byte limit", domain.ErrInvalid)
	}
	b, e := io.ReadAll(io.LimitReader(out.Body, int64(s.MaxBytes)+1))
	if e != nil {
		return nil, e
	}
	if len(b) > s.MaxBytes {
		return nil, fmt.Errorf("%w: WAV exceeds byte limit", domain.ErrInvalid)
	}
	return b, nil
}
