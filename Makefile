.PHONY: test check build integration up up-minio evaluate gallery acceptance
build:
	go build -o bin/repeats-detector ./cmd/repeats-detector
	go build -o bin/evaluate ./cmd/evaluate
	go build -o bin/gallery-evaluate ./cmd/gallery-evaluate
	go build -o bin/acceptance ./cmd/acceptance
	go build -o bin/migrate ./cmd/migrate
	go build -o bin/search-plan ./cmd/search-plan
	go build -o bin/replay-dlq ./cmd/replay-dlq
test:
	go test -race ./...
check:
	go vet ./...
integration:
	go test -race -tags=integration ./... -count=1
up:
	docker compose -f compose.yaml -f compose.minio.yaml -f compose.monitoring.yaml up --build -d
evaluate:
	go run ./cmd/evaluate -manifest $(MANIFEST)

up-minio:
	docker compose -f compose.yaml -f compose.minio.yaml -f compose.monitoring.yaml up --build -d
gallery:
	go run ./cmd/gallery-evaluate -manifest $(MANIFEST) -output $(REPORT)
acceptance:
	go run ./cmd/acceptance -manifest $(MANIFEST) -output $(REPORT)
