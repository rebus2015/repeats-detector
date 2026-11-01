# Настройки окружения

Go-сервис читает ENV процесса через `internal/config`. Файл `.env` самостоятельно
не загружается. Для локального CLI экспортируйте его через shell; Compose передаёт
перечисленные рабочие настройки из `.env` в контейнер через `environment`.
После изменения ENV контейнер сервиса нужно пересоздать.

Примеры: `.env.example` и `deploy/production.env.example`. Новые параметры сохраняют
прежние значения по умолчанию. Длительности используют Go-формат: `100ms`, `10s`, `2m`.
Нулевые и отрицательные интервалы запрещены.

| ENV | По умолчанию | Назначение |
|---|---|---|
| `RABBITMQ_CONNECT_TIMEOUT` | `10s` | Подключение и handshake consumer |
| `RABBITMQ_HEARTBEAT` | `10s` | Предлагаемый heartbeat consumer и publisher; целые секунды от 1 до 65535 |
| `RABBITMQ_RECONNECT_DELAY` | `2s` | Пауза перед восстановлением consumer session |
| `TASK_DELIVERY_LIMIT` | `5` | Порог счётчика доставок в приложении и аргумент quorum queue; 1–1000 |
| `TASK_RETRY_DELAY` | `1s` | Пауза перед возвратом временно неуспешной задачи |
| `OUTBOX_POLL_INTERVAL` | `100ms` | Пауза, когда pending outbox пуст |
| `OUTBOX_RETRY_DELAY` | `2s` | Пауза после ошибки публикации outbox |
| `DELIVERY_POLL_INTERVAL` | `100ms` | Проверка доставленности результата перед Ack задачи |
| `STARTUP_TIMEOUT` | `30s` | Общий бюджет инициализации сервиса |
| `CLEANUP_TIMEOUT` | `30s` | Бюджет прохода retention cleanup |
| `CLEANUP_PRINTS_BATCH_SIZE` | `50000` | Число отпечатков в одной DELETE-транзакции; 1–1000000 |
| `CLEANUP_TASKS_BATCH_SIZE` | `1000` | Число задач в одной DELETE-транзакции; 1–100000 |
| `LOG_LEVEL` | `INFO` | Порог JSON-логов: DEBUG, INFO, WARN, ERROR |

Очистка выполняет последовательные батчи до исчерпания подходящих строк или бюджета
`CLEANUP_TIMEOUT`. Удаление задачи каскадно удаляет связанные данные; размер батча
задач не ограничивает число каскадно удаляемых строк.
`TASK_RETRY_DELAY` ограничивается оставшимся контекстом `TASK_TIMEOUT` и не является
отложенной очередью. Бюджет подключения publisher по-прежнему входит в `PUBLISH_TIMEOUT`.

## PostgreSQL и Compose

`DATABASE_URL` задаёт подключение для сервиса при прямом запуске и для CLI.
`pool_max_conns` в query DSN сохраняется; без него сервис использует `WORKERS + 6`.
Остальные параметры пула продолжает разбирать pgx.

В локальном Compose зависимости доступны по внутренним именам (`postgres`, `rabbitmq`,
`s3mock`/`minio`, `collector`). Поэтому адреса хоста из `.env` не переносятся в контейнер.
Для собственного PostgreSQL DSN контейнера предусмотрен `COMPOSE_DATABASE_URL`, например:

```dotenv
COMPOSE_DATABASE_URL=postgres://repeats:repeats@postgres:5432/repeats?sslmode=disable&pool_max_conns=12
```

`WORKERS`, лимиты, таймауты, очереди, настройки matching, логирования и OpenTelemetry
передаются из `.env` явно. При изменении `HTTP_ADDR` согласуйте проброс порта и healthcheck
в Compose override. Production-шаблон читает `deploy/production.env` через `env_file`.

## Изменение лимита доставок

`TASK_DELIVERY_LIMIT` используется в двух местах: `x-delivery-limit` при объявлении входной
quorum queue и сравнение доступных брокерных счётчиков в приложении. Это порог счётчика,
а не обещание точного количества попыток для всех версий RabbitMQ.

Существующая очередь уже имеет аргументы объявления. Перед изменением значения нужно
согласовать обновление очереди с эксплуатацией: остановить intake, сохранить/обработать
готовые и неподтверждённые сообщения и подготовить очередь с нужными аргументами.
Приложение не удаляет очередь и не переносит сообщения автоматически. Несовместимое
объявление приведёт к ошибке session; readiness останется отрицательной.

## OpenTelemetry

| ENV | Поведение |
|---|---|
| `OTEL_SERVICE_NAME` | Имя сервиса; без ENV — `repeats-detector` |
| `OTEL_RESOURCE_ATTRIBUTES` | Дополнительные resource attributes, например `deployment.environment.name=staging` |
| `OTEL_METRIC_EXPORT_INTERVAL` | Интервал экспорта в **миллисекундах**; без ENV сохраняются 10000 мс |
| `OTEL_TRACES_SAMPLER` | Стандартный sampler SDK, например `parentbased_traceidratio` |
| `OTEL_TRACES_SAMPLER_ARG` | Аргумент sampler, например `0.1` |

Имя и атрибуты читаются через resource detector SDK. Sampling уже обрабатывается SDK.
Некорректные значения стандартных ENV обрабатываются по правилам OpenTelemetry SDK.
Меняя имя сервиса, согласуйте фильтры dashboard и запросы мониторинга.

## CLI и проверки стенда

Явный CLI-флаг имеет приоритет над ENV:

- `acceptance`: `S3_ENDPOINT` → `-s3-endpoint`, `INPUT_QUEUE` → `-queue`.
- `replay-dlq`: `INPUT_QUEUE` → `-input`, `DLQ_QUEUE` → `-dlq`.
- `search-plan` читает конфигурацию сервиса, включая `SEARCH_WINDOW`, `MAX_SEARCH_HITS`,
  `MAX_HASH_POSTINGS`, `MAX_FINGERPRINTS` и проверяет её валидность.

`tools/check_stack.py` и `tools/smoke.py` используют:

| ENV | По умолчанию |
|---|---|
| `SERVICE_URL` | `http://localhost:8080` |
| `RABBITMQ_MANAGEMENT_URL` | `http://localhost:15672` |
| `RABBITMQ_MANAGEMENT_USER`, `RABBITMQ_MANAGEMENT_PASSWORD` | Credentials из `RABBITMQ_URL`, иначе локальные guest/guest |
| `INPUT_QUEUE`, `OUTPUT_QUEUE`, `DLQ_QUEUE` | `audio.tasks`, `audio.results`, `audio.tasks.dead` |
| `GRAFANA_URL`, `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD` | `http://localhost:3000`, `admin`, локальный пароль |
| `PROMETHEUS_URL` | `http://localhost:9090` |
| `S3_ENDPOINT` | `http://localhost:9002` для проверки MinIO health |
| `SMOKE_S3_ENDPOINT`, `SMOKE_BUCKET` | `http://localhost:9000`, `audio` для загрузки в S3Mock |

Vhost management API берётся из `RABBITMQ_URL`; credentials и имена очередей URL-кодируются.
Утилиты читают ENV процесса, а не файл `.env`. Smoke использует загрузку без S3-подписи
и рассчитан на S3Mock. Для MinIO используйте сквозной `acceptance` с AWS SDK.

Формат WAV, контракт сообщений и параметры формирования отпечатков остаются константами
версии алгоритма и не становятся настройками окружения.
