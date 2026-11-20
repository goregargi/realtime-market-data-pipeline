-- Schema for the real-time market data pipeline (TimescaleDB / PostgreSQL 16)

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw trades, partitioned by time. Kept for 7 days.
CREATE TABLE IF NOT EXISTS trades (
    trade_id     BIGINT           NOT NULL,
    product_id   TEXT             NOT NULL,
    price        DOUBLE PRECISION NOT NULL,
    size         DOUBLE PRECISION NOT NULL,
    side         TEXT             NOT NULL CHECK (side IN ('buy', 'sell')),
    trade_time   TIMESTAMPTZ      NOT NULL,
    ingest_time  TIMESTAMPTZ,
    source       TEXT,
    written_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, trade_id, trade_time)
);
SELECT create_hypertable('trades', 'trade_time', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
SELECT add_retention_policy('trades', INTERVAL '7 days', if_not_exists => TRUE);

-- 1-minute candles, upserted as Spark updates each window
CREATE TABLE IF NOT EXISTS candles_1m (
    product_id    TEXT             NOT NULL,
    bucket        TIMESTAMPTZ      NOT NULL,
    open          DOUBLE PRECISION,
    high          DOUBLE PRECISION,
    low           DOUBLE PRECISION,
    close         DOUBLE PRECISION,
    volume        DOUBLE PRECISION,
    vwap          DOUBLE PRECISION,
    buy_volume    DOUBLE PRECISION,
    sell_volume   DOUBLE PRECISION,
    trade_count   BIGINT,
    price_stddev  DOUBLE PRECISION,
    updated_at    TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, bucket)
);
SELECT create_hypertable('candles_1m', 'bucket', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Every rejected record, from either the producer or Spark, with the reason
CREATE TABLE IF NOT EXISTS dead_letter_events (
    id               BIGSERIAL PRIMARY KEY,
    layer            TEXT        NOT NULL,
    reason           TEXT        NOT NULL,
    payload          TEXT,
    kafka_topic      TEXT        NOT NULL,
    kafka_partition  INT         NOT NULL,
    kafka_offset     BIGINT      NOT NULL,
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

-- Per-batch health of each streaming query
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    query_name              TEXT        NOT NULL,
    batch_id                BIGINT      NOT NULL,
    recorded_at             TIMESTAMPTZ NOT NULL,
    input_rows              BIGINT,
    input_rows_per_sec      DOUBLE PRECISION,
    processed_rows_per_sec  DOUBLE PRECISION,
    batch_duration_ms       BIGINT,
    late_rows_dropped       BIGINT,
    PRIMARY KEY (query_name, batch_id)
);

-- Volume spikes: a candle whose volume is more than 3 standard deviations
-- above the previous 30 minutes for the same product
CREATE OR REPLACE VIEW volume_spikes AS
WITH stats AS (
    SELECT
        product_id,
        bucket,
        volume,
        close,
        AVG(volume)    OVER w AS avg_volume_30m,
        STDDEV(volume) OVER w AS std_volume_30m
    FROM candles_1m
    WINDOW w AS (PARTITION BY product_id ORDER BY bucket ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING)
)
SELECT
    product_id,
    bucket,
    close,
    volume,
    avg_volume_30m,
    ROUND(((volume - avg_volume_30m) / NULLIF(std_volume_30m, 0))::numeric, 2) AS z_score
FROM stats
WHERE std_volume_30m > 0
  AND (volume - avg_volume_30m) / std_volume_30m > 3;

-- Freshness: how far behind real time the latest data is, per product
CREATE OR REPLACE VIEW data_freshness AS
SELECT
    product_id,
    MAX(trade_time)                               AS last_trade_time,
    EXTRACT(EPOCH FROM now() - MAX(trade_time))   AS seconds_behind
FROM trades
WHERE trade_time > now() - INTERVAL '1 hour'
GROUP BY product_id;