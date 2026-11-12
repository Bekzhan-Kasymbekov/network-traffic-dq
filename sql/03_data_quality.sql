CREATE SCHEMA IF NOT EXISTS dq;

CREATE TABLE IF NOT EXISTS dq.batch_metrics (
  batch_id UUID PRIMARY KEY,

  source_file TEXT NOT NULL,

  total_records BIGINT NOT NULL,
  valid_records BIGINT NOT NULL,
  rejected_records BIGINT NOT NULL,
  unprocessed_records BIGINT NOT NULL,
  conflicting_records BIGINT NOT NULL,

  rejection_rate NUMERIC(5, 2) NOT NULL,

  calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT nonnegative_counts CHECK (
    total_records >= 0
    AND valid_records >= 0
    AND rejected_records >= 0
    AND unprocessed_records >= 0
    AND conflicting_records >= 0
  ),

  CONSTRAINT valid_rejection_rate CHECK (
    rejection_rate BETWEEN 0 AND 100
  )
);

CREATE TABLE IF NOT EXISTS dq.rule_failures (
  batch_id UUID NOT NULL
    REFERENCES dq.batch_metrics(batch_id),

  reason TEXT NOT NULL,
  occurences BIGINT NOT NULL,

  PRIMARY KEY (batch_id, reason),

  CONSTRAINT nonnegative_occurences
    CHECK (occurences >= 0)
);
