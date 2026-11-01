# Локальный запуск и эксплуатационные процедуры

## Проверка окружения

```sh
docker compose up --build -d
docker compose ps
curl --fail http://localhost:8080/healthz
curl --fail http://localhost:8080/readyz
docker compose logs --tail 100 service collector
python3 tools/generate_corpus.py fixtures
python3 tools/smoke.py
```

Smoke создаёт отдельную scope, загружает два WAV, публикует задачи и проверяет результаты
через RabbitMQ management API. При просмотре результаты возвращаются в очередь.
Можно передать --reference /path/original.wav --query /path/transformed.wav.
Адреса и credentials задаются через ENV, см. [CONFIGURATION.md](CONFIGURATION.md).
Smoke загружает WAV без S3-подписи через `SMOKE_S3_ENDPOINT` и предназначен для S3Mock,
а не для защищённого MinIO. Проверка не предназначена для рабочего окружения.

Остановка без удаления данных: `docker compose stop`. Повторный запуск: `docker compose up -d`.
Именованные volumes сохраняют БД, RabbitMQ и S3Mock. Команды удаления volumes не нужны для обычной остановки.

## DLQ и повторы

DLQ по умолчанию audio.tasks.dead. Проверить JSON, логи по task_id и trace_id, доступность S3/БД/брокера
и ошибки лимитов. Для временного сбоя исправить причину и повторно опубликовать исходный JSON.
Если нужно изменить файл или поля задачи, использовать новый task_id. Перед ручным replay сохранить
исходное событие; удалять его из DLQ только после подтверждённой публикации в очередь входа.

Результаты могут дублироваться: потребитель хранит event_id обработанных событий.
TASK_RETENTION задаёт горизонт идемпотентности и должен превышать максимально возможный срок replay.
Pending outbox не удаляется очисткой. Недоступность брокера не отменяет уже сохранённое решение.
Проверить накопление pending outbox SQL-запросом к rd_outbox и устранить причину публикационных ошибок.

## Обновление алгоритма и схемы

Новая конфигурация извлечения признаков получает новый algorithm_version. Старые и новые отпечатки
не сравниваются. После смены версии новый индекс прогревается по мере поступления задач;
на этот период полнота поиска снижена. Для миграции без такого окна нужен отдельный план двойной
индексации/переиндексации. Повтор существующего task_id продолжает возвращать прежний результат.

Миграция v1 выполняется автоматически под advisory lock. Для следующих версий добавить
версионированную миграцию, проверить совместимость и откат на копии БД. Применять DDL отдельной
ролью в рабочем окружении; не выдавать сервису административные права без необходимости.

## Наблюдаемость

Логи — JSON со связью по trace_id/span_id. Трасса включает загрузку, подготовку PCM, отпечатки,
поиск кандидатов, проверку выравнивания, индексацию и публикацию. Публикация outbox продолжает
контекст задачи. Метрики: audio_processed_total по status, длительность обработки и стадий,
audio_inflight и audio_retries_total.

Для SigNoz заменить debug exporter локального collector на OTLP exporter в нужный endpoint.
Эксплуатационные alerts, dashboards и мониторинг backlog/возраста outbox ещё требуется настроить.
Секреты не хранить в репозитории. Для AWS использовать IAM role с GetObject только нужного bucket/prefix;
сетевые подключения защищать TLS. Проверить backup/restore PostgreSQL и брокера в целевой инфраструктуре.

## Локальный стенд с MinIO и мониторингом

`make up` включает compose.minio.yaml и compose.monitoring.yaml. MinIO API — localhost:9002,
console — localhost:9003; Grafana — localhost:3000; Prometheus — localhost:9090.
Подробные команды и локальные credentials — README.md. Имя RabbitMQ сохраняется в .env.

После устранения причины можно переиграть только нужную область:

```sh
RABBITMQ_URL=amqp://guest:guest@localhost:5672/ bin/replay-dlq -scope exact-scope-id
```

Команда не сбрасывает очередь: чужие сообщения возвращаются в DLQ, broker retry counters
не переносятся, trace context сохраняется. Ack источника следует только после confirm
публикации во вход. Используйте внутри TASK_RETENTION; после его истечения идемпотентность
старой задачи уже не гарантирована. Повреждённые payload сначала исправьте и задайте новый task_id.

Аварийный тест: tools/fault_scenario.py; backup/restore: tools/backup_restore.py.
Метрики и alerts — MONITORING.md, критерии пилота — RELEASE.md.

Проверка доступности и provisioned monitoring: `python3 tools/check_stack.py` (только чтение).
