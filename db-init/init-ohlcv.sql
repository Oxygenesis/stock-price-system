CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS ohlcv (
  ticker TEXT NOT NULL,
  interval TEXT NOT NULL,
  bucket_start TIMESTAMPTZ NOT NULL,
  open DOUBLE PRECISION,
  high DOUBLE PRECISION,
  low DOUBLE PRECISION,
  close DOUBLE PRECISION,
  volume BIGINT,
  PRIMARY KEY (ticker, interval, bucket_start)
);

SELECT timescaledb.create_hypertable('ohlcv', 'bucket_start', if_not_exists => TRUE);