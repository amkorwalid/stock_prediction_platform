-- =============================================================
-- STOCK PREDICTION DATA WAREHOUSE — PostgreSQL Schema
-- Medallion Architecture: Bronze → Silver → Gold → Platinum
-- =============================================================

CREATE SCHEMA IF NOT EXISTS data_warehouse;

-- ==============================================================
-- BRONZE LAYER — Raw Ingestion
-- ==============================================================

CREATE TABLE data_warehouse.raw_ohlcv_prices (
    id            BIGSERIAL       PRIMARY KEY,
    ticker        VARCHAR         NOT NULL,
    trade_date    DATE            NOT NULL,
    open          NUMERIC(18,6)   NOT NULL,
    high          NUMERIC(18,6)   NOT NULL,
    low           NUMERIC(18,6)   NOT NULL,
    close         NUMERIC(18,6)   NOT NULL,
    volume        BIGINT          NOT NULL,
    ingested_at   TIMESTAMP       NOT NULL DEFAULT now()
);

CREATE INDEX idx_raw_ohlcv_ticker_date
    ON data_warehouse.raw_ohlcv_prices (ticker, trade_date);


CREATE TABLE data_warehouse.raw_news_articles (
    article_id    UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    tickers       VARCHAR[]       NOT NULL,   -- Array of mentioned tickers
    headline      TEXT            NOT NULL,
    body          TEXT,
    source        VARCHAR         NOT NULL,
    published_at  TIMESTAMP       NOT NULL,
    lang          VARCHAR(8)      DEFAULT 'en',
    ingested_at   TIMESTAMP       NOT NULL DEFAULT now()
);

CREATE INDEX idx_raw_news_published_at
    ON data_warehouse.raw_news_articles (published_at);


-- ==============================================================
-- SILVER LAYER — Cleansed & Enriched (Star Schema)
-- ==============================================================

-- Slowly-changing dimension for ticker metadata
CREATE TABLE data_warehouse.dim_ticker (
    ticker_id     SERIAL          PRIMARY KEY,
    symbol        VARCHAR(20)     NOT NULL UNIQUE,
    company       VARCHAR         NOT NULL,
    sector        VARCHAR,
    industry      VARCHAR,
    exchange      VARCHAR         NOT NULL,
    currency      VARCHAR(8)      DEFAULT 'USD',
    is_active     BOOLEAN         NOT NULL DEFAULT TRUE,
    listed_at     DATE,
    delisted_at   DATE,
    updated_at    TIMESTAMP       DEFAULT now()
);

COMMENT ON TABLE data_warehouse.dim_ticker
    IS 'Slowly changing dimension for ticker metadata.';


-- Pre-populated calendar table
CREATE TABLE data_warehouse.dim_date (
    date_id         SERIAL      PRIMARY KEY,
    trade_date      DATE        NOT NULL UNIQUE,
    year            SMALLINT    NOT NULL,
    quarter         SMALLINT    NOT NULL,
    month           SMALLINT    NOT NULL,
    week_of_year    SMALLINT    NOT NULL,
    day_of_week     VARCHAR(12) NOT NULL,
    day_of_month    SMALLINT    NOT NULL,
    is_trading_day  BOOLEAN     NOT NULL DEFAULT TRUE,
    is_month_end    BOOLEAN     NOT NULL DEFAULT FALSE,
    is_quarter_end  BOOLEAN     NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE data_warehouse.dim_date
    IS 'Pre-populated calendar table. is_trading_day excludes weekends and market holidays.';


CREATE TABLE data_warehouse.fact_daily_prices (
    price_id      BIGSERIAL       PRIMARY KEY,
    ticker_id     INT             NOT NULL REFERENCES data_warehouse.dim_ticker (ticker_id),
    date_id       INT             NOT NULL REFERENCES data_warehouse.dim_date   (date_id),
    open          NUMERIC(18,6)   NOT NULL,
    high          NUMERIC(18,6)   NOT NULL,
    low           NUMERIC(18,6)   NOT NULL,
    close         NUMERIC(18,6)   NOT NULL,
    adj_close     NUMERIC(18,6)   NOT NULL,   -- Split and dividend adjusted
    volume        BIGINT          NOT NULL,
    vwap          NUMERIC(18,6),
    created_at    TIMESTAMP       DEFAULT now(),

    CONSTRAINT uq_fact_prices_ticker_date UNIQUE (ticker_id, date_id)
);


-- One row per ticker per article; an article mentioning 3 tickers produces 3 rows
CREATE TABLE data_warehouse.fact_news_articles (
    article_id    UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    ticker_id     INT             NOT NULL REFERENCES data_warehouse.dim_ticker (ticker_id),
    date_id       INT             NOT NULL REFERENCES data_warehouse.dim_date   (date_id),
    headline      TEXT            NOT NULL,
    body_clean    TEXT,           -- HTML stripped, normalized whitespace
    published_at  TIMESTAMP       NOT NULL,
    lang          VARCHAR(8)      DEFAULT 'en',
    word_count    INT,
    created_at    TIMESTAMP       DEFAULT now()
);

CREATE INDEX idx_fact_news_ticker_date
    ON data_warehouse.fact_news_articles (ticker_id, date_id);

CREATE INDEX idx_fact_news_published_at
    ON data_warehouse.fact_news_articles (published_at);

COMMENT ON TABLE data_warehouse.fact_news_articles
    IS 'One row per ticker per article. An article mentioning 3 tickers produces 3 rows.';


-- ==============================================================
-- GOLD LAYER — ML-Ready Feature Store & Labels
-- ==============================================================

-- Technical indicators — pre-computed from fact_daily_prices
CREATE TABLE data_warehouse.feat_technical (
    id            BIGSERIAL       PRIMARY KEY,
    ticker_id     INT             NOT NULL REFERENCES data_warehouse.dim_ticker (ticker_id),
    date_id       INT             NOT NULL REFERENCES data_warehouse.dim_date   (date_id),

    -- Moving averages (price level)
    sma_5         NUMERIC(18,6),
    sma_10        NUMERIC(18,6),
    sma_20        NUMERIC(18,6),
    sma_50        NUMERIC(18,6),
    ema_12        NUMERIC(18,6),
    ema_26        NUMERIC(18,6),

    -- Momentum
    rsi_14        NUMERIC(8,4),   -- RSI computed on lagged close diff — no future leakage
    macd          NUMERIC(18,8),  -- (ema12 - ema26) / close, shifted(1)
    macd_signal   NUMERIC(18,8),  -- EWM(span=9) of macd, shifted(1)
    macd_hist     NUMERIC(18,8),  -- macd - macd_signal, shifted(1)

    -- Volatility & bands
    boll_upper    NUMERIC(18,6),
    boll_lower    NUMERIC(18,6),
    boll_mid      NUMERIC(18,6),
    atr_14        NUMERIC(18,6),
    bb_position   NUMERIC(12,8),  -- (close - (bb_mean - 2*bb_std)) / (4*bb_std), shifted(1). Normalized 0-1 within band

    -- Volume
    obv           BIGINT,
    vwap          NUMERIC(18,6),

    computed_at   TIMESTAMP       DEFAULT now(),

    CONSTRAINT uq_feat_tech_ticker_date UNIQUE (ticker_id, date_id)
);

COMMENT ON TABLE data_warehouse.feat_technical
    IS 'Pre-computed from fact_daily_prices. Recomputed nightly. All shifted features are lagged to prevent leakage.';


-- Price dynamics — lagged returns, volatility, momentum
CREATE TABLE data_warehouse.feat_price_dynamics (
    id               BIGSERIAL       PRIMARY KEY,
    ticker_id        INT             NOT NULL REFERENCES data_warehouse.dim_ticker (ticker_id),
    date_id          INT             NOT NULL REFERENCES data_warehouse.dim_date   (date_id),

    -- Raw return (base for all lag/momentum features)
    return_1d        NUMERIC(12,8),  -- close.pct_change() — current day, used to derive lags

    -- Lagged returns — shift(n) of return_1d
    lag_1            NUMERIC(12,8),  -- return.shift(1)
    lag_2            NUMERIC(12,8),  -- return.shift(2)
    lag_3            NUMERIC(12,8),  -- return.shift(3)
    lag_7            NUMERIC(12,8),  -- return.shift(7)
    lag_20           NUMERIC(12,8),  -- return.shift(20)

    -- Multi-horizon returns
    return_5d        NUMERIC(12,8),
    return_20d       NUMERIC(12,8),
    log_return_1d    NUMERIC(12,8),

    -- Rolling return averages
    ma7              NUMERIC(12,8),  -- return.rolling(7).mean()
    ma30             NUMERIC(12,8),  -- return.rolling(30).mean()

    -- Momentum (return minus lagged return)
    momentum_3       NUMERIC(12,8),  -- return - return.shift(3)
    momentum_7       NUMERIC(12,8),  -- return - return.shift(7)

    -- Rolling volatility of returns
    vol_7d           NUMERIC(12,8),  -- return.rolling(7).std() — matches df['volatility'] in feature pipeline
    vol_5d           NUMERIC(12,8),  -- return.rolling(5).std()
    vol_20d          NUMERIC(12,8),  -- return.rolling(20).std()

    -- Volatility of volatility
    vol_of_vol       NUMERIC(12,8),  -- vol_7d.rolling(7).std().shift(1)

    -- Relative price signals
    price_vs_sma50   NUMERIC(12,8),  -- (close - sma50) / sma50
    hl_spread        NUMERIC(12,8),  -- (high - low) / close — intraday range vs close
    daily_range      NUMERIC(12,8),  -- ((high - low) / low).shift(1)
    gap_open         NUMERIC(12,8),  -- (open - prev_close) / prev_close

    -- Volume signals
    volume_ratio     NUMERIC(12,8),  -- volume / avg_volume_20d
    rel_volume       NUMERIC(12,8),
    volume_change    NUMERIC(12,8),  -- volume.pct_change().shift(1)

    computed_at      TIMESTAMP       DEFAULT now(),

    CONSTRAINT uq_feat_dyn_ticker_date UNIQUE (ticker_id, date_id)
);

COMMENT ON TABLE data_warehouse.feat_price_dynamics
    IS 'Derived from fact_daily_prices. All shift(n) features are pre-applied at write time — no further shifting needed at training time.';


-- News sentiment — output of NLP pipeline
CREATE TABLE data_warehouse.feat_news_sentiment (
    id               BIGSERIAL       PRIMARY KEY,
    ticker_id        INT             NOT NULL REFERENCES data_warehouse.dim_ticker (ticker_id),
    date_id          INT             NOT NULL REFERENCES data_warehouse.dim_date   (date_id),
    article_id       UUID         NOT NULL REFERENCES data_warehouse.fact_news_articles (article_id),

    analysis_summary TEXT,

    -- Aggregated sentiment
    sentiment_score  NUMERIC(6,4),   -- -1.0 to 1.0, mean over daily articles
    sentiment_label  VARCHAR(12),    -- UP | DOWN | CONSTANT

    -- Relevance
    relevance        INT,            -- Model confidence that article is stock-relevant

    computed_at      TIMESTAMP       DEFAULT now(),

    CONSTRAINT uq_feat_sent_ticker_date UNIQUE (ticker_id, date_id)
);

COMMENT ON TABLE data_warehouse.feat_news_sentiment
    IS 'Populated by NLP pipeline reading from fact_news_articles. One row per ticker per day.';


-- ==============================================================
-- PLATINUM LAYER — Prediction Serving Layer
-- ==============================================================

-- Model registry
CREATE TABLE data_warehouse.model_registry (
    model_id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    model_name          VARCHAR     NOT NULL,   -- e.g. xgboost_v3_regression
    model_type          VARCHAR     NOT NULL,   -- regression | classification
    version             VARCHAR     NOT NULL,   -- e.g. 3.1.0
    horizon_days        INT         NOT NULL,   -- 1 | 5 | 20
    algorithm           VARCHAR,               -- XGBoost | LSTM | LightGBM | RandomForest | BERT
    feature_set         VARCHAR,               -- price_only | price+news | news_only
    training_start_date DATE,
    training_end_date   DATE,
    is_champion         BOOLEAN     DEFAULT FALSE,  -- Active production model flag
    registered_at       TIMESTAMP   DEFAULT now(),
    notes               TEXT
);

CREATE INDEX idx_champion_model
    ON data_warehouse.model_registry (model_type, horizon_days, is_champion);

CREATE INDEX idx_model_name
    ON data_warehouse.model_registry (model_name);


-- Regression predictions (append-only)
CREATE TABLE data_warehouse.predictions_regression (
    pred_id          UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    ticker_id        INT             NOT NULL REFERENCES data_warehouse.dim_ticker    (ticker_id),
    date_id          INT             NOT NULL REFERENCES data_warehouse.dim_date      (date_id),
    model_id         UUID            NOT NULL REFERENCES data_warehouse.model_registry (model_id),
    horizon_days     INT             NOT NULL,   -- 1 | 5 | 20 — days ahead predicted

    -- Prediction outputs
    pred_close       NUMERIC(12,4),  -- Predicted closing price
    pred_return      NUMERIC(10,6),  -- Predicted % return
    pred_log_return  NUMERIC(10,6),
    confidence_lo    NUMERIC(12,4),  -- Lower bound of prediction interval
    confidence_hi    NUMERIC(12,4),  -- Upper bound of prediction interval
    confidence_level NUMERIC(5,2)    DEFAULT 0.95,  -- e.g. 0.95 for 95% CI

    -- Actuals — filled post-hoc by backtesting job
    actual_close     NUMERIC(12,4),
    actual_return    NUMERIC(10,6),
    mae              NUMERIC(10,6),  -- Filled after outcome is known
    rmse             NUMERIC(10,6),

    -- Metadata — immutable after insert
    predicted_at     TIMESTAMP       NOT NULL DEFAULT now(),
    prediction_date  DATE            NOT NULL,   -- Calendar date prediction was made
    target_date      DATE            NOT NULL,   -- Date being predicted
    data_version     VARCHAR,                    -- Gold snapshot version used for inference

    CONSTRAINT uq_pred_reg UNIQUE (ticker_id, date_id, horizon_days, model_id)
);

CREATE INDEX idx_pred_reg_ticker_date ON data_warehouse.predictions_regression (ticker_id, prediction_date);
CREATE INDEX idx_pred_reg_model       ON data_warehouse.predictions_regression (model_id);
CREATE INDEX idx_pred_reg_ts          ON data_warehouse.predictions_regression (predicted_at);

COMMENT ON TABLE data_warehouse.predictions_regression
    IS 'Append-only. Never update existing rows — insert new rows on retrain.';


-- Classification predictions (append-only)
CREATE TABLE data_warehouse.predictions_classification (
    pred_id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    ticker_id         INT         NOT NULL REFERENCES data_warehouse.dim_ticker     (ticker_id),
    date_id           INT         NOT NULL REFERENCES data_warehouse.dim_date       (date_id),
    model_id          UUID        NOT NULL REFERENCES data_warehouse.model_registry (model_id),
    horizon_days      INT         NOT NULL,   -- 1 | 5 | 20

    -- Prediction outputs
    pred_direction    SMALLINT    NOT NULL,   -- 1 = Up | 0 = Flat | -1 = Down
    prob_up           NUMERIC(6,4) NOT NULL,  -- Probability score 0-1
    prob_flat         NUMERIC(6,4) NOT NULL,
    prob_down         NUMERIC(6,4) NOT NULL,
    pred_confidence   NUMERIC(6,4),           -- max(prob_up, prob_flat, prob_down)
    is_high_confidence BOOLEAN,               -- True if confidence > threshold (e.g. 0.70)

    -- Actuals — filled post-hoc
    actual_direction  SMALLINT,               -- 1 | 0 | -1 — filled after outcome materialises
    is_correct        BOOLEAN,

    -- Metadata — immutable after insert
    predicted_at      TIMESTAMP   NOT NULL DEFAULT now(),
    prediction_date   DATE        NOT NULL,
    target_date       DATE        NOT NULL,
    data_version      VARCHAR,

    CONSTRAINT uq_pred_cls UNIQUE (ticker_id, date_id, horizon_days, model_id),
    CONSTRAINT chk_probs_sum CHECK (
        ABS((prob_up + prob_flat + prob_down) - 1.0) < 0.0001
    )
);

CREATE INDEX idx_pred_cls_ticker_date ON data_warehouse.predictions_classification (ticker_id, prediction_date);
CREATE INDEX idx_pred_cls_model       ON data_warehouse.predictions_classification (model_id);
CREATE INDEX idx_pred_cls_signal      ON data_warehouse.predictions_classification (pred_direction, is_high_confidence);

COMMENT ON TABLE data_warehouse.predictions_classification
    IS 'Append-only. prob_up + prob_flat + prob_down must sum to 1.0 (enforced via CHECK constraint).';


-- Model performance — backtesting & drift monitoring
CREATE TABLE data_warehouse.model_performance (
    perf_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    model_id        UUID        NOT NULL REFERENCES data_warehouse.model_registry (model_id),
    ticker_id       INT         REFERENCES data_warehouse.dim_ticker (ticker_id),  -- NULL = aggregate across all tickers
    model_type      VARCHAR     NOT NULL,   -- Classification | Regression
    eval_start_date DATE,
    eval_end_date   DATE,
    horizon_days    INT         NOT NULL,
    n_samples       INT         NOT NULL,   -- Number of predictions evaluated
    evaluated_at    TIMESTAMP   DEFAULT now()
);

CREATE INDEX idx_perf_model_date ON data_warehouse.model_performance (model_id, evaluated_at, horizon_days);
CREATE INDEX idx_perf_ticker     ON data_warehouse.model_performance (ticker_id);


-- Classification-specific performance metrics
CREATE TABLE data_warehouse.classification_model_performance (
    perf_id         UUID        NOT NULL REFERENCES data_warehouse.model_performance (perf_id),
    accuracy        NUMERIC(6,4),
    precision_score NUMERIC(6,4),
    recall          NUMERIC(6,4),
    f1_score        NUMERIC(6,4),
    auc_roc         NUMERIC(6,4),
    log_loss        NUMERIC(10,6)
);


-- Regression-specific performance metrics
CREATE TABLE data_warehouse.regression_model_performance (
    perf_id         UUID        NOT NULL REFERENCES data_warehouse.model_performance (perf_id),
    mae             NUMERIC(10,6),
    rmse            NUMERIC(10,6)
);