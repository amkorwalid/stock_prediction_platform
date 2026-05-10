
-- ==============================================================
-- SCHEMA: METADATA — Configuration & Pipeline Tracking
-- ==============================================================

CREATE SCHEMA IF NOT EXISTS metadata;


-- ETL pipeline definitions and execution tracking
CREATE TABLE metadata.etl_pipelines (
    pipeline_id                 SERIAL          PRIMARY KEY,
    pipeline_name               VARCHAR(100)    NOT NULL UNIQUE,
    description                 TEXT,
    schedule                    VARCHAR(100),               -- Cron expression: 0 16 * * 1-5
    source_schema               VARCHAR(100),               -- e.g. data_lake
    target_schema               VARCHAR(100),               -- e.g. data_warehouse
    transformation_script       VARCHAR(500),               -- Path to transform script
    depends_on_pipeline_id      INT             REFERENCES metadata.etl_pipelines (pipeline_id),  -- Parent pipeline that must complete first
    timeout_minutes             INT             DEFAULT 60,
    is_active                   BOOLEAN         NOT NULL DEFAULT TRUE,
    status                      VARCHAR(20)     DEFAULT 'idle',  -- idle, running, success, failed
    last_run_start              TIMESTAMP,
    last_run_end                TIMESTAMP,
    last_run_status             VARCHAR(20),
    last_run_duration_seconds   INT,
    last_run_records_processed  INT,
    next_scheduled_run          TIMESTAMP,
    created_at                  TIMESTAMP       NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMP       NOT NULL DEFAULT now()
);

CREATE INDEX idx_pipelines_name     ON metadata.etl_pipelines (pipeline_name);
CREATE INDEX idx_pipelines_active   ON metadata.etl_pipelines (is_active);
CREATE INDEX idx_pipelines_status   ON metadata.etl_pipelines (status);
CREATE INDEX idx_pipelines_next_run ON metadata.etl_pipelines (next_scheduled_run);

COMMENT ON TABLE metadata.etl_pipelines
    IS 'ETL pipeline definitions and execution tracking';


-- Historical log of all pipeline executions
CREATE TABLE metadata.pipeline_runs (
    run_id              SERIAL          PRIMARY KEY,
    pipeline_id         INT             NOT NULL REFERENCES metadata.etl_pipelines (pipeline_id),
    run_start           TIMESTAMP       NOT NULL DEFAULT now(),
    run_end             TIMESTAMP,
    status              VARCHAR(20)     NOT NULL DEFAULT 'running',  -- running, success, failed, cancelled
    records_read        INT             DEFAULT 0,
    records_written     INT             DEFAULT 0,
    records_failed      INT             DEFAULT 0,
    error_message       TEXT,
    error_stack_trace   TEXT,
    execution_details   JSONB,          -- Step-by-step progress
    triggered_by        VARCHAR(50),    -- scheduler, manual, api, dependent_pipeline
    triggered_by_user   VARCHAR(100)
);
 
CREATE INDEX idx_runs_pipeline ON metadata.pipeline_runs (pipeline_id);
CREATE INDEX idx_runs_start    ON metadata.pipeline_runs (run_start);
CREATE INDEX idx_runs_status   ON metadata.pipeline_runs (status);
 
COMMENT ON TABLE metadata.pipeline_runs
    IS 'Historical log of all pipeline executions';