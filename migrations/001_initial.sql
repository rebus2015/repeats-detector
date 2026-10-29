CREATE TABLE IF NOT EXISTS rd_schema (version integer PRIMARY KEY);
CREATE TABLE IF NOT EXISTS rd_tasks (
 scope_id text NOT NULL, task_id text NOT NULL, digest text NOT NULL,
 result jsonb NOT NULL, created_at timestamptz NOT NULL,
 PRIMARY KEY(scope_id, task_id)
);
CREATE TABLE IF NOT EXISTS rd_outbox (
 event_id text PRIMARY KEY, scope_id text NOT NULL, task_id text NOT NULL,
 body jsonb NOT NULL, headers jsonb NOT NULL, created_at timestamptz NOT NULL,
 delivered_at timestamptz,
 FOREIGN KEY(scope_id,task_id) REFERENCES rd_tasks(scope_id,task_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS rd_outbox_pending ON rd_outbox(created_at) WHERE delivered_at IS NULL;
CREATE TABLE IF NOT EXISTS rd_prints (
 scope_id text NOT NULL, task_id text NOT NULL, track_id text NOT NULL,
 version text NOT NULL, hash bigint NOT NULL, frame integer NOT NULL,
 created_at timestamptz NOT NULL,
 PRIMARY KEY(scope_id,task_id,hash,frame),
 FOREIGN KEY(scope_id,task_id) REFERENCES rd_tasks(scope_id,task_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS rd_prints_search ON rd_prints(scope_id,version,hash,created_at);
CREATE INDEX IF NOT EXISTS rd_prints_expiry ON rd_prints(created_at);
INSERT INTO rd_schema(version) VALUES(1) ON CONFLICT DO NOTHING;
