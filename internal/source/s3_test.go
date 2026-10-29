package source

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"repeats-detector/internal/domain"
)

func TestS3BoundedRead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("123456789")) }))
	defer server.Close()
	endpoint := server.URL
	s := S3{Client: s3.New(s3.Options{Region: "test", Credentials: credentials.NewStaticCredentialsProvider("a", "b", ""), BaseEndpoint: &endpoint, UsePathStyle: true}), MaxBytes: 4}
	if _, e := s.Load(context.Background(), "s3://audio/x.wav"); !errors.Is(e, domain.ErrInvalid) {
		t.Fatal("size limit ignored", e)
	}
	if _, e := s.Load(context.Background(), "https://example.com/x.wav"); !errors.Is(e, domain.ErrInvalid) {
		t.Fatal("unsupported location accepted", e)
	}
}
