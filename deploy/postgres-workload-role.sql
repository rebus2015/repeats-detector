-- Run as the database owner after migrations in a dedicated repeats database.
-- Provision LOGIN/password outside this file using the platform's secret manager.
CREATE ROLE repeats_worker NOLOGIN;
GRANT CONNECT ON DATABASE repeats TO repeats_worker;
GRANT USAGE ON SCHEMA public TO repeats_worker;
GRANT SELECT ON rd_schema TO repeats_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON rd_tasks, rd_prints, rd_outbox TO repeats_worker;
-- Grant repeats_worker to the provisioned runtime LOGIN role.
-- No table ownership, schema CREATE, or superuser privileges are required at runtime.
