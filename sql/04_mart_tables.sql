CREATE TABLE IF NOT EXISTS mart.protocol_statistics (
  batch_id UUID NOT NULL,

  proto TEXT NOT NULL,
  label SMALLINT NOT NULL,

  flow_count BIGINT NOT NULL,

  total_source_bytes BIGINT NOT NULL,
  total_destination_bytes BIGINT NOT NULL,

  avg_duration DOUBLE PRECISION NOT NULL,

  calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY(batch_id, proto, label),

  CHECK (label IN (0,1)),
  CHECK (flow_count >= 0),
  CHECK (total_source_bytes >= 0),
  CHECK (total_destination_bytes >= 0)
);

CREATE TABLE IF NOT EXISTS mart.attack_statistics (
  batch_id UUID NOT NULL,

  attack_cat TEXT NOT NULL,

  flow_count BIGINT NOT NULL,

  calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (batch_id, attack_cat),

  CHECK (flow_count >= 0)
);
