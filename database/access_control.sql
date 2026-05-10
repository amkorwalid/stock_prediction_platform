CREATE ROLE dw_reader;
CREATE ROLE dw_writer;
CREATE ROLE metadata_rw;
CREATE ROLE audit_inserter;
CREATE ROLE audit_reader;

-- =============================================================================
-- Stock Prediction Platform — PostgreSQL Users, Roles & Permissions
-- Database: PostgreSQL 14+
-- Generated from: erd.dbml + c4.dsl
--
-- Architecture Components:
--   • backend_user   → FastAPI (reads DW, writes audit)
--   • airflow_user   → Apache Airflow (full metadata, reads DW, writes audit)
--   • etl_user       → Python ETL Scripts (writes Bronze/Silver/Gold DW)
--   • ml_user        → Python ML Scripts (reads features, writes predictions)
--   • readonly_user  → Traders/Investors via API (SELECT on serving layer only)
--   • audit_writer   → Dedicated audit log writer (INSERT only on audit schema)
--   • superadmin     → DBA / DevOps (full access, maintenance only)
-- =============================================================================

CREATE USER backend_user
    WITH PASSWORD ''
    LOGIN;

COMMENT ON ROLE backend_user IS
    'FastAPI backend service account. Read-only on data_warehouse, write-only on audit.';

-- -----------------------------------------------------------------------------
-- backend_user — FastAPI Backend API
--   Reads: data_warehouse (dim_*, fact_*, feat_*, predictions_*, model_*)
--   Writes: audit.access_log, audit.system_logs
-- -----------------------------------------------------------------------------

CREATE USER airflow_user
    WITH PASSWORD ''
    LOGIN;

COMMENT ON ROLE airflow_user IS
    'Airflow orchestrator service account. Full access to metadata schema.';

-- -----------------------------------------------------------------------------
-- airflow_user — Apache Airflow Orchestrator
--   Reads/Writes: metadata.etl_pipelines, metadata.pipeline_runs
--   Writes: audit.system_logs
-- -----------------------------------------------------------------------------

CREATE USER etl_user
    WITH PASSWORD ''
    LOGIN;

COMMENT ON ROLE etl_user IS
    'ETL pipeline service account. Writes raw and enriched data into data_warehouse.';

-- -----------------------------------------------------------------------------
-- etl_user — Python ETL / Data Fetching & Preprocessing Scripts
--   Writes: data_warehouse.raw_ohlcv_prices, raw_news_articles,
--           dim_ticker, dim_date, fact_daily_prices, fact_news_articles,
--           feat_price_dynamics, feat_technical, feat_news_sentiment
--   Writes: audit.system_logs
-- -----------------------------------------------------------------------------

CREATE USER ml_user
    WITH PASSWORD ''
    LOGIN;

COMMENT ON ROLE ml_user IS
    'ML training and inference service account. Reads features, writes predictions and model registry.';

-- -----------------------------------------------------------------------------
-- ml_user — Python ML Training & Inference Scripts
--   Reads:  data_warehouse.feat_price_dynamics, feat_technical, feat_news_sentiment,
--           dim_ticker, dim_date, model_registry, fact_daily_prices, fact_news_articles,
--   Writes: data_warehouse.predictions_regression,
--           predictions_classification, model_performance,
--           classification_model_performance, regression_model_performance,
--           model_registry (INSERT new models)
--   Writes: audit.system_logs
-- -----------------------------------------------------------------------------

CREATE USER readonly_user
    WITH PASSWORD ''
    LOGIN;

COMMENT ON ROLE readonly_user IS
    'Read-only account for trader/investor-facing API queries on serving layer.';

-- -----------------------------------------------------------------------------
-- readonly_user — End-user read access (Traders & Investors via API)
--   Reads: data_warehouse Silver + Gold + Platinum layers only
-- -----------------------------------------------------------------------------

CREATE USER audit_writer
    WITH PASSWORD ''
    LOGIN;

COMMENT ON ROLE audit_writer IS
    'Dedicated audit schema writer. INSERT-only on audit tables.';

-- -----------------------------------------------------------------------------
-- audit_writer — Dedicated audit log writer
--   Writes: audit.access_log, audit.system_logs
-- -----------------------------------------------------------------------------

CREATE USER superadmin
    WITH PASSWORD ''
    SUPERUSER
    CREATEROLE
    CREATEDB
    CONNECTION LIMIT 5
    LOGIN;

COMMENT ON ROLE superadmin IS
    'DBA/DevOps superuser. For maintenance only — never used by application services.';

-- -----------------------------------------------------------------------------
-- superadmin — DBA / DevOps maintenance account
--   Full access. Must NOT be used by any application service.
-- -----------------------------------------------------------------------------

GRANT USAGE ON SCHEMA data_warehouse TO dw_reader, dw_writer, backend_user, etl_user, ml_user, readonly_user;
GRANT USAGE ON SCHEMA metadata        TO metadata_rw, airflow_user;
GRANT USAGE ON SCHEMA audit           TO audit_inserter, audit_reader, audit_writer,
                                        backend_user, etl_user, ml_user, airflow_user;

-- =============================================================================
-- SECTION 4: TABLE-LEVEL PERMISSIONS PER ROLE
-- =============================================================================

-- 4.1  dw_reader — SELECT on all data_warehouse tables

GRANT SELECT ON ALL TABLES IN SCHEMA data_warehouse TO dw_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_warehouse
    GRANT SELECT ON TABLES TO dw_reader;

-- 4.2  dw_writer — Full DML on data_warehouse tables

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA data_warehouse TO dw_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA data_warehouse TO dw_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_warehouse
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO dw_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_warehouse
    GRANT USAGE, SELECT ON SEQUENCES TO dw_writer;

-- 4.3  metadata_rw — Full DML on metadata schema

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA metadata TO metadata_rw;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA metadata TO metadata_rw;
ALTER DEFAULT PRIVILEGES IN SCHEMA metadata
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO metadata_rw;
ALTER DEFAULT PRIVILEGES IN SCHEMA metadata
    GRANT USAGE, SELECT ON SEQUENCES TO metadata_rw;

-- 4.4  audit_inserter — INSERT only on audit tables

GRANT INSERT ON audit.access_log  TO audit_inserter;
GRANT INSERT ON audit.system_logs TO audit_inserter;

GRANT SELECT ON ALL TABLES IN SCHEMA audit TO audit_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit
    GRANT SELECT ON TABLES TO audit_reader;

-- =============================================================================
-- SECTION 5: USER → ROLE ASSIGNMENTS
-- =============================================================================

-- backend_user: reads DW + writes audit

GRANT dw_reader      TO backend_user;
GRANT audit_inserter TO backend_user;

-- airflow_user: full metadata + reads DW + writes audit

GRANT metadata_rw    TO airflow_user;
GRANT dw_reader      TO airflow_user;
GRANT audit_inserter TO airflow_user;

-- etl_user: writes to DW (Bronze/Silver/Gold) + writes audit

GRANT dw_writer      TO etl_user;
GRANT audit_inserter TO etl_user;

-- ml_user: reads features + writes predictions/model tables + writes audit

GRANT dw_writer      TO ml_user;
GRANT audit_inserter TO ml_user;

-- readonly_user: SELECT only on serving layer (Silver/Gold/Platinum)

GRANT dw_reader      TO readonly_user;

-- audit_writer: INSERT only on audit

GRANT audit_inserter TO audit_writer;

-- =============================================================================
-- SECTION 6: FINE-GRAINED REVOKES (least-privilege hardening)
-- =============================================================================

-- ml_user must NOT write to ingestion/feature tables

REVOKE INSERT, UPDATE, DELETE
    ON data_warehouse.feat_price_dynamics,
       data_warehouse.feat_news_sentiment,
       data_warehouse.raw_ohlcv_prices,
       data_warehouse.raw_news_articles,
       data_warehouse.fact_daily_prices,
       data_warehouse.fact_news_articles,
       data_warehouse.feat_technical,
       data_warehouse.dim_ticker,
       data_warehouse.dim_date
    FROM ml_user;

-- etl_user must NOT write to prediction or model tables

REVOKE INSERT, UPDATE, DELETE
    ON data_warehouse.predictions_regression,
       data_warehouse.predictions_classification,
       data_warehouse.model_registry,
       data_warehouse.model_performance,
       data_warehouse.classification_model_performance,
       data_warehouse.regression_model_performance
    FROM etl_user;

-- readonly_user must NOT see raw Bronze layer (privacy + performance)

REVOKE SELECT
    ON data_warehouse.raw_ohlcv_prices,
       data_warehouse.raw_news_articles
    FROM readonly_user;

-- =============================================================================
-- SECTION 7: SEQUENCE GRANTS
-- =============================================================================

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA audit
    TO audit_inserter, backend_user, etl_user, ml_user, airflow_user, audit_writer;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA metadata
    TO metadata_rw, airflow_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA audit
    GRANT USAGE, SELECT ON SEQUENCES TO audit_inserter;

-- =============================================================================
-- SECTION 8: DEFAULT SEARCH PATHS
-- =============================================================================

ALTER ROLE backend_user  SET search_path = data_warehouse, audit, public;
ALTER ROLE airflow_user  SET search_path = metadata, data_warehouse, audit, public;
ALTER ROLE etl_user      SET search_path = data_warehouse, audit, public;
ALTER ROLE ml_user       SET search_path = data_warehouse, audit, public;
ALTER ROLE readonly_user SET search_path = data_warehouse, public;
ALTER ROLE audit_writer  SET search_path = audit, public;


-- =============================================================================

-- Grant all privileges on airflow_metadata schema
GRANT ALL PRIVILEGES ON SCHEMA airflow_metadata TO airflow_user;

-- Grant create privilege on schema
GRANT CREATE ON SCHEMA airflow_metadata TO airflow_user;

-- Grant all on all tables in the schema
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA airflow_metadata TO airflow_user;

-- Grant all on all sequences
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA airflow_metadata TO airflow_user;

-- Set default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA airflow_metadata 
    GRANT ALL ON TABLES TO airflow_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA airflow_metadata 
    GRANT ALL ON SEQUENCES TO airflow_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA airflow_metadata 
    GRANT ALL ON FUNCTIONS TO airflow_user;

-- Set search path for airflow_user
ALTER USER airflow_user SET search_path TO airflow_metadata, public;
