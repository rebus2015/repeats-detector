# Наблюдение и оповещения

Для локального стенда `compose.monitoring.yaml` запускает Prometheus и Grafana,
автоматически подключает источник данных, dashboard и alert rules. `make up` включает
этот overlay. Grafana: http://localhost:3000, Prometheus /alerts: http://localhost:9090/alerts.
Внешняя доставка уведомлений не настроена: локально состояние правил доступно в UI.

Локальный collector принимает OTLP и собирает Prometheus-метрики RabbitMQ с порта 15692.
`deploy/otel-collector.signoz.yaml` отправляет их в SigNoz через TLS; задайте
SIGNOZ_ENDPOINT (host:4317) и SIGNOZ_INGESTION_KEY. В частном окружении установите доверенный CA.
Файл конфигурации подготовлен; подключение к конкретному SigNoz и доставка оповещений
проверяются владельцем окружения после задания адресов и получателей.

`deploy/dashboard.grafana.json` содержит 12 панелей в формате Grafana. Его можно
импортировать в Grafana либо через поддерживаемый SigNoz импорт Grafana JSON:
https://signoz.io/docs/userguide/manage-dashboards/ . Выражения PromQL и пороги
также сохранены в `deploy/alerts.prometheus.yaml`. Импорт в целевой SigNoz и наличие
конкретных имён histogram-метрик ещё требуют проверки на его версии.

Создайте dashboard с фильтром service.name = repeats-detector. Не используйте task_id,
track_id и scope_id как метки метрик: это создаст неограниченную кардинальность.

| Панель | Метрика / агрегация | Предварительное оповещение |
|---|---|---|
| Результаты в секунду | rate(audio_processed_total), группировка status | error + rejected + retry_exhausted > 1% за 5 минут, при >=100 задачах |
| Задержка | p50/p95/p99 audio_processing_duration_seconds | p95 > 5 секунд за 10 минут для задач <=30 секунд |
| Этапы | p95 audio_stage_duration_seconds по stage | увеличение относительно исходного профиля |
| Активная обработка | audio_inflight | насыщение одновременно с ростом очереди |
| Повторы | rate(audio_retries_total) | >0 в течение 5 минут |
| Outbox | audio_outbox_pending, audio_outbox_oldest_seconds | возраст >30 секунд 2 минуты; >120 секунд — критическое |
| Мониторинг БД | rate(audio_monitor_errors_total) | >0 в течение 1 минуты |
| Память Go | audio_go_heap_bytes, audio_go_goroutines | рост без стабилизации; RSS дополнительно от контейнерного агента |
| Очереди | rabbitmq_queue_messages_ready / unacked | вход >1000 за 5 минут; DLQ >0 |
| Связность | scrape up, /readyz, отсутствие метрик сервиса | недоступность >1 минуты |

Имена метрик могут нормализоваться экспортёром (точки/суффиксы); выбирайте фактические имена
в Metrics Explorer. В локальном rabbitmq.conf включены per-object metrics: проверяйте наличие метки queue
при переносе конфигурации. Без неё агрегированные данные не позволяют отдельно наблюдать DLQ.

Histogram OTel настраивается средствами backend; score результата не является вероятностью.
После timeout/overflow ищите trace с Candidate_Search и Match_Verify. При росте outbox
проверьте RabbitMQ publisher confirms и доступность выходной очереди. Не очищайте outbox
ради снятия оповещения. Получатели результатов обязаны устранять повторы по event_id.

Указанные пороги — исходные настройки для пилота, не утверждённый SLA. Сам dashboard
и каналы уведомлений в пользовательском SigNoz ещё не созданы: адрес окружения не задан.
