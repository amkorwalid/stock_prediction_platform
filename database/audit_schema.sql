
-- ==============================================================
-- SCHEMA: AUDIT — Compliance & Security Tracking
-- ==============================================================
 
CREATE SCHEMA IF NOT EXISTS audit;
 
 
-- API access log for security monitoring and analytics
CREATE TABLE audit.access_log (
    access_id              SERIAL          PRIMARY KEY,
    username               VARCHAR(100),
    resource               VARCHAR(500)    NOT NULL,   -- API endpoint or database table
    action                 VARCHAR(50)     NOT NULL,   -- SELECT, GET, POST, PUT, DELETE
    access_status          VARCHAR(20)     NOT NULL,   -- success, denied, error
    status_code            INT,                        -- HTTP status code
    ip_address             INET            NOT NULL,
    user_agent             TEXT,
    request_method         VARCHAR(10),
    request_path           VARCHAR(500),
    request_query_params   JSONB,
    request_body_summary   TEXT,                       -- Truncated/sanitized request body
    response_time_ms       INT,
    error_message          TEXT,
    accessed_at            TIMESTAMP       NOT NULL DEFAULT now()
);
 
CREATE INDEX idx_access_time     ON audit.access_log (accessed_at);
CREATE INDEX idx_access_status   ON audit.access_log (access_status);
CREATE INDEX idx_access_resource ON audit.access_log (resource);
CREATE INDEX idx_access_ip       ON audit.access_log (ip_address);
 
COMMENT ON TABLE audit.access_log
    IS 'API access log for security monitoring and analytics';
 
 
-- Application logs for debugging and monitoring
CREATE TABLE audit.system_logs (
    log_id       SERIAL          PRIMARY KEY,
    log_level    VARCHAR(10)     NOT NULL,   -- DEBUG, INFO, WARNING, ERROR, CRITICAL
    component    VARCHAR(100)    NOT NULL,   -- backend, pipeline, ml_inference, etc.
    module       VARCHAR(100),               -- Specific module/file name
    message      TEXT            NOT NULL,
    stack_trace  TEXT,
    metadata     JSONB,                      -- Additional context
    created_at   TIMESTAMP       NOT NULL DEFAULT now()
);
 
CREATE INDEX idx_logs_level     ON audit.system_logs (log_level);
CREATE INDEX idx_logs_component ON audit.system_logs (component);
CREATE INDEX idx_logs_created   ON audit.system_logs (created_at);
 
COMMENT ON TABLE audit.system_logs
    IS 'Application logs for debugging and monitoring';