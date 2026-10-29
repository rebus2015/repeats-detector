# repeats-detector

Сервис поиска повторяющихся аудиофрагментов. Аудиофайлы хранятся в **MinIO**, задачи со
ссылками на них поступают через **RabbitMQ**. Сервис загружает WAV в оперативную память,
вычисляет акустические отпечатки, ищет совпадения в **PostgreSQL** и публикует результат
в выходную очередь RabbitMQ.

Целевой сценарий — найти **тот же звуковой фрагмент** после обрезки, перекодирования
или добавления шума. Рабочий вход — PCM WAV; декодирование сжатых форматов внутри
сервиса не выполняется. Повторно произнесённый текст является другой аудиозаписью.

Текущий результат — рабочий локальный пилот с Docker Compose, тестами, мониторингом,
проверками восстановления и измерениями на длинных записях. Состояние на 30 октября 2026:
[STATUS.md](docs/STATUS.md).

## Содержание

- [Назначение](#назначение)
- [Состав и архитектура](#состав-и-архитектура)
- [Формат аудио](#формат-аудио)
- [Пайплайн обработки](#пайплайн-обработки)
- [Алгоритм отпечатков](#алгоритм-отпечатков)
- [Запуск локального стенда](#запуск-локального-стенда)
- [Адреса и учётные данные](#адреса-и-учётные-данные)
- [Пример обработки файла](#пример-обработки-файла)
- [Контракт сообщений](#контракт-сообщений)
- [Настройки](#настройки)
- [Надёжность и хранение](#надёжность-и-хранение)
- [Мониторинг](#мониторинг)
- [Структура проекта](#структура-проекта)
- [Разработка и тестирование](#разработка-и-тестирование)
- [Тестовые данные и результаты](#тестовые-данные-и-результаты)
- [Эксплуатационные команды](#эксплуатационные-команды)
- [Ограничения и дальнейшая работа](#ограничения-и-дальнейшая-работа)
- [Дополнительная документация](#дополнительная-документация)

## Назначение

Сервис отвечает на вопрос: встречался ли достаточно длинный участок этой записи
в ранее обработанном аудио той же области поиска за последние 60 минут?

Например, сначала поступает запись совещания, а затем — вырезанный из неё фрагмент
с добавленным шумом. Для второй задачи сервис может вернуть `duplicate`, указать
ранее индексированную запись и приблизительные позиции совпадения.

`duplicate` означает наличие совпавшего участка. Остальная часть файла может содержать
новое аудио. Результат содержит один подтверждённый участок; суммарная доля повторного
содержимого и все интервалы совпадений пока не вычисляются.

`scope_id` разделяет независимые области поиска: клиентов, проекты или потоки обработки.
Записи из разных `scope_id` между собой не сравниваются.

## Состав и архитектура

| Компонент | Роль |
|---|---|
| Go service | Получение задач, чтение WAV, извлечение отпечатков, принятие решения, публикация результатов |
| MinIO | Хранение исходных и тестовых аудиофайлов; доступ по S3 API |
| RabbitMQ | Входная очередь, выходная очередь, повторы и очередь ошибок DLQ |
| PostgreSQL | Индекс отпечатков, результаты задач, идемпотентность и transactional outbox |
| OpenTelemetry Collector | Приём телеметрии сервиса, сбор метрик RabbitMQ и экспорт метрик |
| Prometheus | Хранение и запросы метрик, вычисление правил оповещений |
| Grafana | Готовые панели мониторинга локального стенда |
| S3Mock | Дополнительный эмулятор S3 для прежних тестов; основной стенд использует MinIO |

```mermaid
flowchart LR
    Producer[Источник задач] -->|JSON со s3_url| Input[RabbitMQ: audio.tasks]
    Producer -->|WAV| MinIO[MinIO]
    Input --> Worker[Go service]
    MinIO -->|S3 GetObject в RAM| Worker
    Worker -->|Поиск и транзакция| PG[(PostgreSQL: tasks, prints, outbox)]
    PG --> Publisher[Outbox publisher]
    Publisher -->|Результат и publisher confirm| Output[RabbitMQ: audio.results]
    Worker -->|Ошибочные задачи| DLQ[RabbitMQ: audio.tasks.dead]
    Worker -->|OTLP| Collector[OTel Collector]
    Rabbit[RabbitMQ metrics] --> Collector
    Collector --> Prometheus[Prometheus]
    Prometheus --> Grafana[Grafana]
```

Отдельная векторная БД, GPU и Qdrant в текущей реализации не используются.
Поиск основан на совпадении дискретных акустических хешей и проверке временного
выравнивания. PostgreSQL одновременно хранит индекс и обеспечивает согласованность
решения, новой индексации и события результата.

## Формат аудио

Сервис принимает следующие параметры:

| Параметр | Требование |
|---|---|
| Контейнер | RIFF/WAVE (`.wav`) |
| Кодирование | Signed PCM, little-endian, 16 бит |
| Частота дискретизации | 16 000 Гц |
| Каналы | 1, mono |
| Расположение | `s3://bucket/key` в MinIO или другом совместимом S3-хранилище |
| Размер по умолчанию | До 128 000 000 байт, включая заголовок |

Поддерживаются дополнительные RIFF chunks и PCM-заголовки WAVE_FORMAT_EXTENSIBLE.
Параметры проверяются по содержимому файла. Расширение `.wav` само по себе не подтверждает
подходящий формат. MP3, AAC, Opus, stereo, float PCM и другая частота на рабочем входе
не принимаются. Автоматическое преобразование входа не предусмотрено.

Проверенный диапазон запросов начинается с **5 секунд**, устойчивее результаты от
**10 секунд**. Настройка `MIN_MATCH_SECONDS=2` относится к длительности подтверждённого
участка. Запрос ровно в две секунды после исключения граничных кадров обычно не проходит
этот порог.

При обработке аудио не сохраняется во временные файлы на диске. При этом исходный WAV,
список отпечатков и ограниченный набор кандидатов занимают оперативную память.
Дополнительный лимит числа отпечатков может сработать раньше лимита размера WAV.

## Пайплайн обработки

1. **Получение задачи.** Worker читает сообщение из `audio.tasks` с ручным подтверждением.
   Проверяет JSON, идентификаторы и версию контракта.
2. **Проверка повторной доставки.** Для уже завершённой задачи возвращается сохранённый
   результат. Повторная загрузка объекта и вычисление отпечатков не требуются.
3. **Чтение из MinIO.** По `s3_url` вызывается S3 `GetObject`. Размер проверяется по
   `Content-Length`, если он указан, и во время чтения потока.
4. **Подготовка PCM.** Проверяются RIFF chunks, формат, частота, число каналов и целостность
   данных. PCM представляет срез исходного буфера без дополнительного копирования аудио.
5. **Извлечение отпечатков.** Строятся спектральные ориентиры и хеши временных пар.
6. **Поиск кандидатов.** В PostgreSQL ищутся равные хеши той же версии и `scope_id`,
   проиндексированные в последние 60 минут по времени БД.
7. **Проверка совпадений.** Проверяются общий временной сдвиг, число подтверждённых кадров,
   длительность, покрытие и допустимые разрывы.
8. **Атомарное сохранение.** Одной транзакцией фиксируются результат, отпечатки
   информативной записи и событие в outbox. Частичные повторы также индексируются,
   чтобы сохранять новый материал для следующих запросов.
9. **Публикация результата.** Отдельный publisher отправляет событие из outbox
   в `audio.results`, получает подтверждение брокера и отмечает доставку в БД.
10. **Подтверждение входа.** Worker делает Ack исходного сообщения после проверки
    подтверждённой публикации результата.

Для длинных файлов поиск выполняется перекрывающимися окнами, по умолчанию по 10 секунд
с перекрытием 2,8 секунды. Слишком частые хеши фильтруются. Если исключено более половины
разных хешей окна, включается запасной поиск без фильтра с тем же лимитом кандидатов.
При переполнении окно уменьшается; если этого недостаточно, обработка завершается ошибкой
без фиксации `unique` по неполной выдаче.

## Алгоритм отпечатков

Извлечение реализовано собственным Go-кодом в
[internal/fingerprint/landmark.go](internal/fingerprint/landmark.go).

| Параметр | Значение и смысл |
|---|---|
| Версия | `landmark-v2-temporal-fft1024-hop256` |
| FFT | 1024 отсчёта, окно анализа 64 мс |
| Шаг | 256 отсчётов, 16 мс |
| Оконная функция | Hann |
| Спектральные ориентиры | До трёх локальных пиков на кадр |
| Окрестность максимума | ±3 кадра и ±3 частотных бина |
| Временные пары | До шести следующих подходящих ориентиров на расстоянии 4–64 кадра |
| Квантование частоты | По два FFT-бина, примерно 31,25 Гц |
| Квантование интервала | По два кадра, 32 мс |

Хеш упаковывает частоты двух ориентиров и расстояние между ними по времени.
Абсолютная позиция хранится отдельно: обрезка начала записи меняет временной сдвиг,
сохраняя часть относительных признаков.

Это акустический признак, а не криптографический хеш всего файла. Отдельные совпадения
хешей возможны у разных записей, поэтому решение принимает временной verifier в
[internal/matching/match.go](internal/matching/match.go). Один подтверждённый кадр должен
поддерживаться минимум двумя разными хешами. `score` описывает покрытие интервала такими
кадрами и не является вероятностью совпадения.

Обоснования параметров и этапы алгоритма прокомментированы в исходном файле.
Архитектурные детали — [ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Запуск локального стенда

### Требования

- Docker с Docker Compose v2.
- Доступ к сети при первой загрузке образов и сборке MinIO/Go service.
- Для разработки на компьютере: Go 1.26+; для Python-утилит — Python 3.
- AWS CLI нужен только для приведённого ниже примера ручной загрузки файла.

Для запуска контейнеров установленный на компьютере Go не требуется.

### Полный стенд

Из корня проекта:

```sh
make up
```

Эквивалентная команда:

```sh
docker compose -f compose.yaml -f compose.minio.yaml -f compose.monitoring.yaml up --build -d
```

Проверка готовности и списка контейнеров:

```sh
curl --fail http://localhost:8080/readyz
docker compose -f compose.yaml -f compose.minio.yaml -f compose.monitoring.yaml ps
```

`/readyz` при успехе возвращает HTTP 200 с пустым телом: пустая страница в браузере ожидаема.
При неготовности возвращается HTTP 503. Проверяются PostgreSQL, активный consumer RabbitMQ
и отсутствие завершения процесса. `/healthz` проверяет живой HTTP-процесс.
Задачи обработки поступают через RabbitMQ; HTTP-порт используется для этих проверок.

В полном стенде объединяются три файла:

| Файл | Назначение |
|---|---|
| [compose.yaml](compose.yaml) | PostgreSQL, RabbitMQ, сервис, Collector и прежний S3Mock |
| [compose.minio.yaml](compose.minio.yaml) | Добавляет настоящий MinIO и направляет S3-клиент сервиса на него |
| [compose.monitoring.yaml](compose.monitoring.yaml) | Добавляет Prometheus и Grafana |

При запуске одного `compose.yaml` сервис подключается к S3Mock. Для рабочего локального
сценария с MinIO используйте полную команду или `make up`.

### Запуск Go-процесса на компьютере

Запустите только зависимости, освободив порт 8080, если контейнер сервиса уже работал:

```sh
docker compose -f compose.yaml -f compose.minio.yaml -f compose.monitoring.yaml stop service
docker compose -f compose.yaml -f compose.minio.yaml -f compose.monitoring.yaml up -d postgres rabbitmq minio collector prometheus grafana
cp -n .env.example .env
set -a
. ./.env
set +a
go run ./cmd/repeats-detector
```

`cp -n` сохраняет существующий `.env`, включая закреплённое имя узла RabbitMQ.
Go-приложение читает окружение процесса; загрузку `.env` выполняет shell в командах выше.
При таком запуске адрес OTLP из примера указывает на опубликованный порт Collector.

## Адреса и учётные данные

### Доступ с компьютера

Все опубликованные порты локального стенда привязаны к `127.0.0.1`.

| Сервис | Endpoint | Назначение |
|---|---|---|
| Go service | [http://localhost:8080/healthz](http://localhost:8080/healthz) | Живой процесс |
| Go service | [http://localhost:8080/readyz](http://localhost:8080/readyz) | Готовность к обработке |
| MinIO API | [http://localhost:9002](http://localhost:9002) | S3 API для файлов |
| MinIO Console | [http://localhost:9003](http://localhost:9003) | Управление bucket и объектами |
| RabbitMQ | `amqp://localhost:5672/` | Протокол сообщений |
| RabbitMQ UI / API | [http://localhost:15672](http://localhost:15672) | Очереди, публикации, управление |
| RabbitMQ metrics | [http://localhost:15692/metrics](http://localhost:15692/metrics) | Метрики брокера |
| PostgreSQL | `localhost:55432`, база `repeats` | SQL-подключение |
| OTel Collector | `localhost:4317` | OTLP по gRPC |
| Grafana | [http://localhost:3000](http://localhost:3000) | Мониторинг |
| Dashboard | [http://localhost:3000/d/repeats-detector](http://localhost:3000/d/repeats-detector) | Панели проекта, папка Audio |
| Prometheus | [http://localhost:9090](http://localhost:9090) | Метрики и PromQL |
| Alert rules | [http://localhost:9090/alerts](http://localhost:9090/alerts) | Состояние правил |
| S3Mock | [http://localhost:9000](http://localhost:9000) | Эмулятор для прежних тестов |

### Локальные учётные данные

| Сервис | Логин / Access Key | Пароль / Secret Key |
|---|---|---|
| Grafana | `admin` | `repeats-local-grafana` |
| RabbitMQ | `guest` | `guest` |
| MinIO | `repeats-local` | `repeats-local-secret` |
| PostgreSQL | `repeats` | `repeats` |
| Prometheus, Collector, health/readiness | Без авторизации | — |
| S3Mock | Без проверки учётных данных | — |

Для Grafana пароль первого запуска можно задать через `GRAFANA_ADMIN_PASSWORD`.
Уже изменённый пароль хранится в томе Grafana; таблица описывает исходные локальные настройки.
Эти данные предназначены для стенда разработки. Эксплуатационный шаблон использует
отдельные секреты и TLS.

Строки подключения с компьютера:

```dotenv
DATABASE_URL=postgres://repeats:repeats@localhost:55432/repeats?sslmode=disable
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
S3_ENDPOINT=http://localhost:9002
AWS_ACCESS_KEY_ID=repeats-local
AWS_SECRET_ACCESS_KEY=repeats-local-secret
AWS_REGION=us-east-1
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
```

### Адреса внутри Docker-сети

Внутри контейнера `localhost` обозначает сам контейнер. Между сервисами используются
имена Docker Compose:

| Компонент | Внутренний адрес |
|---|---|
| Go service | `http://service:8080` |
| MinIO API | `http://minio:9000` |
| MinIO Console | `http://minio:9001` |
| RabbitMQ | `amqp://guest:guest@rabbitmq:5672/` |
| RabbitMQ UI / API | `http://rabbitmq:15672` |
| RabbitMQ metrics | `http://rabbitmq:15692/metrics` |
| PostgreSQL | `postgres://repeats:repeats@postgres:5432/repeats?sslmode=disable` |
| OTel Collector | `http://collector:4317`, OTLP gRPC |
| Метрики Collector для Prometheus | `http://collector:8889/metrics`, порт снаружи не опубликован |
| Prometheus | `http://prometheus:9090` |
| Grafana | `http://grafana:3000` |
| S3Mock | `http://s3mock:9090` |

**SigNoz локально не установлен.** Используются Grafana и Prometheus. Для будущего подключения
к SigNoz подготовлен [otel-collector.signoz.yaml](deploy/otel-collector.signoz.yaml).
Адреса и учётной записи SigNoz в текущем стенде нет.

## Пример обработки файла

### 1. Подготовить bucket и загрузить WAV

При первом запуске создайте bucket `audio`:

```sh
AWS_ACCESS_KEY_ID=repeats-local AWS_SECRET_ACCESS_KEY=repeats-local-secret \
  aws --endpoint-url http://localhost:9002 --region us-east-1 s3 mb s3://audio
```

Если bucket уже существует, переходите к загрузке:

```sh
AWS_ACCESS_KEY_ID=repeats-local AWS_SECRET_ACCESS_KEY=repeats-local-secret \
  aws --endpoint-url http://localhost:9002 --region us-east-1 \
  s3 cp recording.wav s3://audio/recordings/recording-001.wav
```

То же можно сделать через MinIO Console. Для сервиса ссылка на объект будет
`s3://audio/recordings/recording-001.wav`. Она не содержит HTTP endpoint: адрес MinIO
сервис берёт из `S3_ENDPOINT`.

### 2. Отправить задачу

В RabbitMQ UI откройте очередь `audio.tasks` и секцию публикации сообщения.
Укажите JSON из примера ниже. При публикации через default exchange routing key
должен быть `audio.tasks`.

```json
{
  "schema_version": 1,
  "task_id": "task-001",
  "track_id": "recording-001",
  "scope_id": "example-session-1",
  "s3_url": "s3://audio/recordings/recording-001.wav"
}
```

Эквивалентная публикация через Management HTTP API:

```sh
curl --fail --user guest:guest \
  --header 'Content-Type: application/json' \
  --request POST \
  'http://localhost:15672/api/exchanges/%2F/amq.default/publish' \
  --data-binary @- <<'JSON'
{
  "properties": {"delivery_mode": 2, "content_type": "application/json"},
  "routing_key": "audio.tasks",
  "payload": "{\"schema_version\":1,\"task_id\":\"task-001\",\"track_id\":\"recording-001\",\"scope_id\":\"example-session-1\",\"s3_url\":\"s3://audio/recordings/recording-001.wav\"}",
  "payload_encoding": "string"
}
JSON
```

Ответ `{"routed":true}` подтверждает маршрутизацию сообщения в очередь.
Обработку и публикацию результата проверяйте отдельно.

### 3. Прочитать результат

Откройте очередь `audio.results` в RabbitMQ UI. Для просмотра через HTTP API
с возвратом сообщений в очередь:

```sh
curl --fail --user guest:guest \
  --header 'Content-Type: application/json' \
  --request POST \
  'http://localhost:15672/api/queues/%2F/audio.results/get' \
  --data '{"count":10,"ackmode":"ack_requeue_true","encoding":"auto"}'
```

Для информативной записи без совпадений ожидается `unique`.
Затем загрузите вырезанный участок под другим ключом и отправьте новую задачу
с новыми `task_id` и `track_id`, сохранив `scope_id`. Подтверждённый повтор даст `duplicate`.

Изменённый файл должен получать новый идентификатор записи. Повторяйте неизменённую
задачу с прежним `task_id` для повторной доставки, а новую обработку оформляйте новым `task_id`.

## Контракт сообщений

### Очереди

| Очередь по умолчанию | Назначение |
|---|---|
| `audio.tasks` | Входные задачи; durable quorum queue |
| `audio.results` | Результаты; durable queue |
| `audio.tasks.dead` | DLQ: некорректные и исчерпавшие повторы задачи |

### Поля входа

| Поле | Смысл |
|---|---|
| `schema_version` | Версия контракта, поддерживается `1`; при отсутствии используется `1` |
| `task_id` | Идентификатор неизменяемой задачи внутри области поиска |
| `track_id` | Идентификатор неизменяемой аудиозаписи |
| `scope_id` | Область поиска; при отсутствии — `default` |
| `s3_url` | URI объекта вида `s3://bucket/key` |

Идентификаторы непустые, длиной до 256 байт. Максимальный размер JSON сообщения — 16 KiB.
HTTP и presigned URL в `s3_url` не принимаются.

### Форма результата

Пример результата с иллюстративными временными позициями и оценкой:

```json
{
  "schema_version": 1,
  "event_id": "b20b0d83840cba16e6e5b4382920d2e84ff7ca15d2c2253f66166fbdcf8c99fe",
  "task_id": "task-002",
  "track_id": "recording-002",
  "scope_id": "example-session-1",
  "status": "duplicate",
  "algorithm_version": "landmark-v2-temporal-fft1024-hop256",
  "indexed_at": "2026-10-30T09:00:00Z",
  "match_details": {
    "original_track_id": "recording-001",
    "query_start_sec": 0.048,
    "original_start_sec": 1.392,
    "matched_duration_sec": 4.656,
    "score": 0.42,
    "evidence": 123
  }
}
```

| Статус | Смысл |
|---|---|
| `unique` | Подтверждённого совпадения нет; информативные отпечатки сохранены |
| `duplicate` | Найден хотя бы один достаточно длинный совпавший участок |
| `insufficient_audio` | Длительности или разнообразия признаков недостаточно; результат опубликован, аудио не индексируется |

`query_start_sec` и `original_start_sec` — приблизительные начала подтверждённого участка
в текущей и ранее индексированной записи. При стандартном оконном поиске возвращаемый
участок обычно не длиннее 10 секунд. `score` — покрытие интервала подтверждёнными кадрами,
`evidence` — число таких кадров. `original_track_id` указывает ранее индексированную запись;
каноническое происхождение всех копий не строится.

`event_id` стабильно вычисляется из `scope_id` и `task_id`; AMQP `MessageId` результата
равен `event_id`. Получатель должен устранять повторные события по этому идентификатору.
Полный контракт — [CONTRACTS.md](docs/CONTRACTS.md).

## Настройки

Полный пример окружения — [.env.example](.env.example).

### Подключения

| Переменная | Локальная настройка для процесса на компьютере |
|---|---|
| `DATABASE_URL` | `postgres://repeats:repeats@localhost:55432/repeats?sslmode=disable`; обязательна |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` |
| `S3_ENDPOINT` | `http://localhost:9002` для MinIO |
| `AWS_ACCESS_KEY_ID` | `repeats-local` |
| `AWS_SECRET_ACCESS_KEY` | `repeats-local-secret` |
| `AWS_REGION` | `us-east-1` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` |

При незаданном `S3_ENDPOINT` используется стандартный AWS S3 endpoint и цепочка
AWS credentials, включая workload role. Для своего MinIO задайте его endpoint и ключи
доступа; SDK использует path-style addressing при явном endpoint.

### Параметры приложения

Значения по умолчанию из [internal/config/config.go](internal/config/config.go):

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `INPUT_QUEUE` | `audio.tasks` | Вход |
| `OUTPUT_QUEUE` | `audio.results` | Результаты |
| `DLQ_QUEUE` | `audio.tasks.dead` | Ошибочные задачи |
| `HTTP_ADDR` | `:8080` | Адрес HTTP health/readiness |
| `WORKERS` | `2` | Параллельные обработчики; максимум 64 |
| `MAX_WAV_BYTES` | `128000000` | Лимит загружаемого WAV в байтах |
| `MAX_FINGERPRINTS` | `1000000` | Лимит отпечатков на файл |
| `MAX_SEARCH_HITS` | `200000` | Лимит кандидатов на поисковое окно |
| `MAX_HASH_POSTINGS` | `32` | Порог частоты хеша в активном индексе области поиска |
| `TASK_TIMEOUT` | `2m` | Таймаут обработки и ожидания публикации результата |
| `PUBLISH_TIMEOUT` | `10s` | Таймаут публикации результата |
| `SHUTDOWN_TIMEOUT` | `30s` | Время на завершение активных обработчиков |
| `SEARCH_WINDOW` | `60m` | Возраст записей, участвующих в поиске |
| `INDEX_RETENTION` | `90m` | Возраст отпечатков для физической очистки |
| `TASK_RETENTION` | `168h` | Хранение задач и окно идемпотентности |
| `CLEANUP_INTERVAL` | `5m` | Интервал запуска очистки |
| `MIN_MATCH_SECONDS` | `2` | Минимальная подтверждённая длительность |
| `MIN_MATCH_FRAMES` | `12` | Минимальное число подтверждённых кадров |
| `MIN_MATCH_COVERAGE` | `0.08` | Минимальное покрытие интервала |
| `MAX_MATCH_GAP_SECONDS` | `0.8` | Максимальный разрыв между подтверждёнными кадрами |
| `AUTO_MIGRATE` | `true` | Выполнить миграцию при старте; при `false` проверить версию схемы |
| `REQUIRE_TLS` | `false` | Потребовать verify-full для PostgreSQL, amqps и HTTPS S3/OTLP |

`OTEL_SDK_DISABLED=true` отключает SDK телеметрии. `GRAFANA_ADMIN_PASSWORD` и
`RABBITMQ_HOSTNAME` относятся к Compose. Имя узла RabbitMQ нужно сохранять неизменным
при повторном использовании его тома; в текущем локальном `.env` закреплено прежнее имя.

Приложение проверяет границы параметров: например, retention индекса не может быть
меньше окна поиска, а срок хранения задач — меньше retention индекса.

### Как изменить параметры контейнера

Compose читает `.env` для подстановок в YAML. Переменная из `.env` попадает в окружение
Go-контейнера только при соответствующей настройке `environment` или `env_file`.
В текущих Compose-файлах параметры подключений заданы в `service.environment`,
а многие рабочие параметры используют значения по умолчанию приложения.

Например, создайте `compose.local.yaml`:

```yaml
services:
  service:
    environment:
      WORKERS: "4"
      MAX_WAV_BYTES: "128000000"
```

Примените его последним:

```sh
docker compose -f compose.yaml -f compose.minio.yaml -f compose.monitoring.yaml -f compose.local.yaml up -d service
```

## Надёжность и хранение

В PostgreSQL находятся четыре основные таблицы:

| Таблица | Содержимое |
|---|---|
| `rd_schema` | Версия схемы |
| `rd_tasks` | Входной digest, сохранённый результат и время задачи |
| `rd_prints` | Хеши, временные кадры, track, scope, версия алгоритма и время индексации |
| `rd_outbox` | События результата, trace headers и отметка доставки |

Решение и запись индекса сериализованы транзакционной advisory lock внутри `scope_id`.
Это позволяет двум одновременно пришедшим копиям увидеть результаты друг друга после
последовательного принятия решений. Разные области поиска используют независимые блокировки.
Загрузка S3 и FFT выполняются вне этой транзакции.

Повтор неизменённой задачи в пределах `TASK_RETENTION` возвращает прежний результат.
Изменение любого поля при прежних `scope_id` и `task_id` считается конфликтом и ведёт в DLQ.
Повторная доставка не обновляет время индексации.

Доставка результатов — **at-least-once**. Сбой после подтверждения брокера, но до отметки
outbox, может привести к повторной публикации с тем же `event_id`. Получатель отвечает
за идемпотентное применение результата.

Невалидные JSON/WAV, конфликт задачи и превышение размера отклоняются в DLQ.
Временные ошибки повторяются с паузой. Лимит учитывает quorum delivery counters;
после исчерпания повторов задача также попадает в DLQ. Переполнение поиска не считается
отсутствием совпадений.

Поиск ограничен 60 минутами независимо от физической очистки. По умолчанию каждые
5 минут удаляются отпечатки старше 90 минут и задачи старше 7 дней без недоставленных
outbox-событий. Очистка проходит несколькими ограниченными батчами в пределах временного
бюджета. Момент физического удаления может отставать от порога.

MinIO продолжает хранить исходные WAV: очистка PostgreSQL не удаляет объекты из bucket.
Их срок хранения задаётся отдельно средствами хранилища.

## Мониторинг

Локальный маршрут телеметрии: **Go / RabbitMQ → OTel Collector → Prometheus → Grafana**.
Traces принимаются по OTLP и выводятся debug exporter Collector. Отдельный интерфейс
просмотра traces в текущий Compose не добавлен.

Dashboard `Repeats detector — operations` автоматически загружается в папку `Audio`.
Он содержит 12 панелей: скорость обработки, задержка p95, outbox pending/age,
активные задачи, повторы, heap, горутины, очереди, DLQ и ошибки мониторинга.

| Метрика | Что показывает |
|---|---|
| `audio_processed_total` | Результаты и ошибки по статусу |
| `audio_processing_duration_seconds` | Длительность обработки, histogram |
| `audio_stage_duration_seconds` | Длительности этапов |
| `audio_inflight` | Активные обработчики |
| `audio_retries_total` | Повторы обработки |
| `audio_outbox_pending` | Число недоставленных событий |
| `audio_outbox_oldest_seconds` | Возраст самого старого недоставленного события |
| `audio_monitor_errors_total` | Ошибки чтения состояния outbox |
| `audio_go_heap_bytes` | Память Go heap |
| `audio_go_goroutines` | Число горутин |
| `rabbitmq_queue_messages_ready` | Готовые к выдаче сообщения, с метками очередей |

Prometheus загружает пять правил: задержка outbox, критическая задержка outbox,
доля ошибок обработки, непустая DLQ и ошибки мониторинга. Состояние доступно в `/alerts`.
Внешние получатели уведомлений локально не настроены.

Логи сервиса — JSON; для активных traces добавляются `trace_id` и `span_id`.
`traceparent` передаётся через AMQP headers.

```sh
docker compose -f compose.yaml -f compose.minio.yaml -f compose.monitoring.yaml logs -f service collector
python3 tools/check_stack.py
```

Настройки: [MONITORING.md](docs/MONITORING.md),
[dashboard.grafana.json](deploy/dashboard.grafana.json),
[alerts.prometheus.yaml](deploy/alerts.prometheus.yaml).

## Структура проекта

```text
cmd/
  repeats-detector/       Основной сервис
  evaluate/              Попарная оценка алгоритмов без инфраструктуры
  gallery-evaluate/      Поиск по общей неизменной RAM-галерее
  acceptance/            Сквозной тест MinIO/RabbitMQ/сервиса/outbox
  migrate/               Отдельный запуск миграции
  search-plan/           EXPLAIN ANALYZE поиска по настоящим отпечаткам
  replay-dlq/            Повторная отправка задач выбранного scope
internal/
  audio/                 Разбор PCM WAV
  fingerprint/           FFT и спектральные отпечатки
  matching/              Проверка временных совпадений
  source/                Ограниченное чтение S3 в память
  store/                 PostgreSQL, поиск, транзакции, outbox, очистка
  usecase/               Оркестрация обработки одной задачи
  worker/                AMQP consumer и publisher
  config/                ENV и проверка настроек
  observability/         OpenTelemetry и JSON-логи
  domain/                Задача, результат и модели совпадений
  testaudio/             Сигналы для тестов
migrations/              SQL-схема
```

Дополнительные каталоги:

| Путь | Содержимое |
|---|---|
| `deploy/` | Collector, Grafana/Prometheus, сборка MinIO, policies и production template |
| `tools/` | Генерация данных, аудит совпадений, проверки стенда, аварий и backup |
| `docs/` | Архитектура, контракты, эксплуатация и результаты измерений |
| `.github/workflows/ci.yml` | Сборка, проверки Go, integration и синтетическая оценка |
| `bin/` | Собранные CLI, исключены из Git |
| `fixtures/` | Производные тестовые данные и manifests, исключены из Git |
| `reports/` | Полные локальные отчёты, исключены из Git |
| `backups/` | Локальные архивы БД, исключены из Git и Docker build context |

Схема встроена в приложение из `internal/store/schema.sql`; SQL-копия хранится в
`migrations/001_initial.sql`. При дальнейшем изменении схемы нужны версионированные миграции.

## Разработка и тестирование

Сборка всех CLI и быстрые проверки:

```sh
make build
make test
make check
go mod verify
```

Интеграционные тесты требуют запущенных PostgreSQL и RabbitMQ:

```sh
TEST_DATABASE_URL='postgres://repeats:repeats@localhost:55432/repeats?sslmode=disable' \
TEST_RABBITMQ_URL='amqp://guest:guest@localhost:5672/' \
  go test -race -tags=integration ./... -count=1
```

Тесты создают временные схемы БД и отдельные очереди. Проверяются конкурентные решения,
идемпотентность и конфликты, outbox, временное окно, частичные повторы, ограничения поиска,
частотный fallback, очистка нескольких батчей, ограниченная роль БД, DLQ/replay,
повторное использование AMQP-соединения и зависание Channel.Open после handshake.
Один из pipeline tests читает аудио через настоящий AWS SDK из локального HTTP fixture server.

Benchmark извлечения отпечатков:

```sh
go test ./internal/fingerprint -run '^$' -bench . -benchmem
```

CI запускает Go checks и integration tests, проверяет Python-синтаксис, собирает команды
и выполняет оценку на синтетическом корпусе. Пользовательский речевой корпус в CI не включён.

## Тестовые данные и результаты

### Локальные источники

| Путь в текущем окружении | Содержимое |
|---|---|
| `/Users/rebus/Projects/audio-test-data/data/` | Прежний набор коротких речевых WAV |
| `/Users/rebus/Projects/audio-test-data/audio/` | Новый набор: 216 WAV, 20,30 часа |
| `fixtures/speech/` | Создаётся утилитой подготовки короткой речевой выборки |
| `fixtures/long/` | Создаётся утилитой подготовки длинного корпуса: запросы и manifest |

Абсолютные пути относятся к компьютеру разработки. На другой машине передайте свой путь
к WAV в утилиту подготовки. Генераторы сохраняют производные файлы отдельно; исходники
не изменяются и не входят в репозиторий.

Сгенерированные выборки, отчёты, JSON-сводки измерений, локальные backup и бинарники
удалены при очистке проекта. Каталоги `fixtures/`, `reports/`, `backups/` и `bin/`
создаются заново соответствующими командами. Приведённые ниже результаты описывают
прошедшие прогоны; исходный код тестов и утилиты воспроизведения сохранены.

### Автономная проверка длинной галереи

```sh
python3 tools/prepare_long_corpus.py /path/to/audio fixtures/long --negative-recordings 8
make build
mkdir -p reports
bin/gallery-evaluate -manifest fixtures/long/manifest.json -output reports/gallery.json
python3 tools/audit_gallery_matches.py fixtures/long/manifest.json reports/gallery.json reports/shared-content.json
```

Подготовка выбирает около часа исходников в галерею и другие записи для предполагаемых
неповторов. Создаются обрезки 2/5/10/30 секунд из разных позиций, с шумом 18/8 dB
и без добавленного шума. Manifest содержит SHA256, seed и точные позиции.
RAM-галерея остаётся неизменной во время запросов.

### Сквозная проверка

Экспортируйте подключение к локальным зависимостям из `.env`, затем запустите:

```sh
set -a
. ./.env
set +a
mkdir -p reports
bin/acceptance -manifest fixtures/long/manifest.json -queries 216 -concurrency 4 -output reports/acceptance.json
```

Команда создаёт bucket при необходимости, загружает аудио под отдельным префиксом
и использует новый случайный `scope_id`. Проверяются результат в БД и доставленный outbox.
Объекты и результаты после прогона сохраняются. При пропуске известной копии команда
возвращает ненулевой exit code и сохраняет отчёт.

Сквозной индекс пополняется запросами, поэтому его recall отличается от оценки
на статической галерее. Дополнительно можно проверить две независимые области одновременно:

```sh
python3 tools/multiscope_load.py --parallel 2 --queries 30
```

### Фактические результаты длинного корпуса

Заголовки всех 216 файлов соответствуют требуемому формату. Для оценки выбраны 8 файлов
в галерею — 63 минуты и 1,16 млн отпечатков — и 8 файлов предполагаемых неповторов.
Это проверка выбранных 16 исходников, а не оценка качества на всех 216 файлах.

| Проверка | Результат |
|---|---|
| Статическая галерея, копии >=5 секунд | 205 / 216, 94,91% |
| Обрезки без добавленного шума, >=5 секунд | 72 / 72 |
| Статическая галерея, 2 секунды | 0 / 72 |
| Сквозной тест, копии >=5 секунд | 214 / 216; два пропуска |
| Сквозная задержка, 4 outstanding / 2 workers | p95 3,86 с; p99 8,85 с |
| Два независимых scope, по 30 запросов | 60 / 60; p95 около 2,04 с |
| SIGKILL service / RabbitMQ / PostgreSQL | После восстановления 30 / 30 повторов |
| Backup/restore в отдельную БД | 7,69 млн отпечатков; ссылочная целостность сохранена |

Найдены совпадения между разными файлами с общим звуковым содержимым. Корреляционный
аудит подтвердил общий PCM-контент. Поэтому разметка «другой файл = неповтор» не позволяет
по этим данным достоверно оценить precision. Показатели задержки относятся к локальному
стенду и этому профилю нагрузки; эксплуатационный SLA не установлен.

Полные условия и сводки: [LONG_AUDIO.md](docs/evaluation/LONG_AUDIO.md).

### Прежние и синтетические проверки

```sh
python3 tools/generate_corpus.py fixtures
bin/evaluate -manifest fixtures/manifest.json -method landmarks
bin/evaluate -manifest fixtures/manifest.json -method landmarks-coarse
bin/evaluate -manifest fixtures/manifest.json -method exact-pcm
```

`exact-pcm` — контроль точного совпадения PCM, `landmarks-coarse` — эксперимент с более
грубым частотным квантованием. Рабочий сервис использует `landmarks`.
Генератор может добавлять преобразования с кодеками, если доступны ffmpeg или audioop;
на вход оценке подаются декодированные WAV. Это отдельные эксперименты устойчивости,
формат рабочего входа сервиса остаётся неизменным.

Подготовка старого короткого корпуса:

```sh
python3 tools/prepare_speech_corpus.py /path/to/wav-folder fixtures/speech
bin/evaluate -manifest fixtures/speech/holdout.json
```

На macOS `--aac` добавляет эксперимент с AAC через `afconvert`. Исторические результаты
сохранены в [evaluation/README.md](docs/evaluation/README.md) отдельно от длинного корпуса.

## Эксплуатационные команды

### Проверка состояния и логов

```sh
python3 tools/check_stack.py
docker compose -f compose.yaml -f compose.minio.yaml -f compose.monitoring.yaml logs --tail 100 service
```

`check_stack.py` выполняет только чтение: проверяет readiness, MinIO, dashboard,
правила, наличие метрик и состояние основных очередей.

### Работа с DLQ

Сначала устраните причину ошибки. Повторно отправить задачи одной области:

```sh
RABBITMQ_URL=amqp://guest:guest@localhost:5672/ \
  bin/replay-dlq -scope example-session-1
```

Команда проверяет до 1000 готовых сообщений за запуск. Чужие сообщения возвращаются
в DLQ. Счётчики предыдущих доставок не переносятся, trace context сохраняется.
Ack сообщения DLQ следует после подтверждённой публикации во входную очередь.
При недоступном маршруте исходное сообщение остаётся в DLQ.

Replay с тем же неизменённым payload выполняйте в пределах `TASK_RETENTION`.
Исправленному содержимому задачи назначайте новый `task_id`.

### Диагностика SQL

Для существующего тестового `scope_id`:

```sh
DATABASE_URL='postgres://repeats:repeats@localhost:55432/repeats?sslmode=disable' \
  bin/search-plan -scope existing-test-scope -wav query.wav
```

Команда выполняет `EXPLAIN ANALYZE` на реальных отпечатках первых десяти секунд WAV.
Запрос действительно исполняется; данные не меняются.

### Миграции

В локальном стенде `AUTO_MIGRATE=true`. Отдельный запуск с правами владельца схемы:

```sh
DATABASE_URL='postgres://repeats:repeats@localhost:55432/repeats?sslmode=disable' \
  bin/migrate
```

При `AUTO_MIGRATE=false` сервис только проверяет версию схемы и может работать
под ролью без DDL-прав. Пример grants —
[postgres-workload-role.sql](deploy/postgres-workload-role.sql).

### Аварийные проверки и резервная копия

Следующая проверка временно прерывает локальные контейнеры этого проекта,
сохраняя их тома:

```sh
python3 tools/fault_scenario.py --output reports/faults.json
```

Создание архива и проверка восстановления в отдельную временную БД:

```sh
python3 tools/backup_restore.py --archive backups/repeats.dump --report reports/backup-restore.json
```

Существующий архив не перезаписывается. Проверочная БД удаляется после проверки;
рабочая БД не заменяется. Архив включает состояние PostgreSQL, а хранение объектов MinIO
и данные RabbitMQ требуют отдельных процедур.

Если `docker` отсутствует в PATH на macOS, обеим утилитам можно передать:

```text
--docker /Applications/Docker.app/Contents/Resources/bin/docker
```

### Остановка

```sh
docker compose -f compose.yaml -f compose.minio.yaml -f compose.monitoring.yaml stop
```

`down` удаляет контейнеры и сеть, сохраняя именованные тома. Удаление томов через `-v`
удалит сохранённые данные. После остановки повторный запуск выполняется через `make up`.

## Ограничения и дальнейшая работа

- Текущие пороги не обеспечивают обнаружение двухсекундных запросов. Короткие зашумлённые
  участки также могут пропускаться; два пропуска сквозного теста сохранены в отчёте.
- Для подтверждения precision нужен вручную проверенный корпус неповторов. Отдельно
  нужно проверить одинаковый текст, произнесённый заново, и реальные фоновые помехи.
- Не реализованы автоматическая настройка порогов, рабочий приём сжатых форматов,
  поиск с изменением скорости/высоты тона и возврат всех совпавших интервалов.
- Внутри одного `scope_id` принятие решений последовательное. Длительный нагрузочный
  прогон и предел производительности пока не измерены.
- Локальные process-crash проверки не подтверждают устойчивость к сетевым разделениям
  кластера. Внешняя доставка alerts, эксплуатационные TLS endpoint и RPO/RTO пока не проверены.

Для перехода к целевому окружению подготовлены
[compose.production.yaml](deploy/compose.production.yaml),
[production.env.example](deploy/production.env.example), ограниченные S3/SQL policies
и [RELEASE.md](docs/RELEASE.md). В текущую задачу входит локальный Docker Compose.

## Дополнительная документация

| Документ | Содержание |
|---|---|
| [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md) | План разработки и история этапов |
| [docs/STATUS.md](docs/STATUS.md) | Реализованные возможности и незакрытые задачи |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Устройство алгоритма, транзакций и ограничения ресурсов |
| [docs/CONTRACTS.md](docs/CONTRACTS.md) | Поля событий, статусы и семантика доставки |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Запуск, DLQ, обновления и восстановление |
| [docs/MONITORING.md](docs/MONITORING.md) | Метрики, dashboard, alerts и подключение SigNoz |
| [docs/RELEASE.md](docs/RELEASE.md) | Критерии пилота, роли, TLS и подготовка выпуска |
| [docs/evaluation/LONG_AUDIO.md](docs/evaluation/LONG_AUDIO.md) | Новый длинный корпус и сквозные измерения |
| [docs/evaluation/README.md](docs/evaluation/README.md) | Исторические результаты короткой выборки |
