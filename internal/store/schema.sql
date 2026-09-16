CREATE SCHEMA IF NOT EXISTS queue;
CREATE SCHEMA IF NOT EXISTS telemetry;
CREATE TABLE IF NOT EXISTS queue.capacity (
 id integer PRIMARY KEY CHECK (id = 1), outstanding bigint NOT NULL CHECK (outstanding >= 0)
);
INSERT INTO queue.capacity VALUES (1,0) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS queue.messages (
 event_id text PRIMARY KEY,
 sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
 payload jsonb,
 payload_hash text NOT NULL,
 state text NOT NULL DEFAULT 'ready' CHECK (state IN ('ready','leased','done','dead')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 lease_token text NOT NULL DEFAULT '',
 lease_until timestamptz,
 attempts integer NOT NULL DEFAULT 0,
 last_error text NOT NULL DEFAULT '',
 completed_at timestamptz
);
CREATE INDEX IF NOT EXISTS messages_ready ON queue.messages (available_at, sequence) WHERE state='ready';
CREATE INDEX IF NOT EXISTS messages_leased ON queue.messages (lease_until) WHERE state='leased';
CREATE INDEX IF NOT EXISTS messages_done ON queue.messages (completed_at) WHERE state='done';
CREATE TABLE IF NOT EXISTS telemetry.gpus (
 uuid text PRIMARY KEY, host text NOT NULL, gpu_index integer NOT NULL,
 device text NOT NULL, model text NOT NULL
);
CREATE TABLE IF NOT EXISTS telemetry.observations (
 event_id text PRIMARY KEY,
 gpu_uuid text NOT NULL REFERENCES telemetry.gpus(uuid),
 processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 metric_name text NOT NULL, value double precision NOT NULL,
 source jsonb NOT NULL,
 payload_hash text NOT NULL
);
CREATE INDEX IF NOT EXISTS observations_gpu_time ON telemetry.observations(gpu_uuid, processed_at, event_id);
