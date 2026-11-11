CREATE TABLE IF NOT EXISTS clean.network_flows (
  raw_id BIGINT PRIMARY KEY
    REFERENCES raw.network_flows(raw_id),

  batch_id UUID NOT NULL,

  flow_id INTEGER NOT NULL,

  dur DOUBLE PRECISION NOT NULL,

  proto TEXT NOT NULL,
  service TEXT,
  state TEXT NOT NULL,

  spkts INTEGER NOT NULL,
  dpkts INTEGER NOT NULL,

  sbytes BIGINT NOT NULL,
  dbytes BIGINT NOT NULL,

  attack_cat TEXT,
  label SMALLINT NOT NULL,

  cleaned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT valid_duration
    CHECK (dur >= 0 AND dur < 'Infinity'::DOUBLE PRECISION),

  CONSTRAINT valid_packet_counts
    CHECK (spkts >= 0 AND dpkts >= 0),

  CONSTRAINT valid_byte_counts
    CHECK (sbytes >= 0 AND dbytes >= 0),

  CONSTRAINT valid_label
    CHECK (label IN (0, 1))
);

CREATE TABLE IF NOT EXISTS clean.rejected_flows (
  raw_id BIGINT PRIMARY KEY
    REFERENCES raw.network_flows(raw_id),

  batch_id UUID NOT NULL,

  rejection_reasons JSONB NOT NULL,

  rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
