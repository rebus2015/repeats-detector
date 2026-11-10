FROM golang:1.27.3-alpine AS build
WORKDIR /src
# Слой зависимостей переиспользуется при изменениях в исходном коде.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /service ./cmd/repeats-detector

# Финальный образ содержит готовый бинарник и запускает его от пользователя app.
FROM alpine:3.22
RUN adduser -D -u 10001 app
USER app
COPY --from=build /service /service
# Путь к папке с PCM передаётся аргументом docker run.
ENTRYPOINT ["/service"]
