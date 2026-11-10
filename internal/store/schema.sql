-- Версия схемы для запуска с отключённой миграцией.
CREATE TABLE IF NOT EXISTS rd_schema (version integer PRIMARY KEY);

-- Сохранённый результат файла в пределах одной области поиска.
CREATE TABLE IF NOT EXISTS rd_tasks (
 scope_id text NOT NULL, task_id text NOT NULL, digest text NOT NULL,
 result jsonb NOT NULL, created_at timestamptz NOT NULL,
 PRIMARY KEY(scope_id, task_id)
);

-- Поисковый индекс хранит отпечатки вместе с версией алгоритма и номером кадра.
CREATE TABLE IF NOT EXISTS rd_prints (
 scope_id text NOT NULL, task_id text NOT NULL, track_id text NOT NULL,
 version text NOT NULL, hash bigint NOT NULL, frame integer NOT NULL,
 created_at timestamptz NOT NULL,
 PRIMARY KEY(scope_id,task_id,hash,frame),
 FOREIGN KEY(scope_id,task_id) REFERENCES rd_tasks(scope_id,task_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS rd_prints_search ON rd_prints(scope_id,version,hash,created_at);
INSERT INTO rd_schema(version) VALUES(1) ON CONFLICT DO NOTHING;
