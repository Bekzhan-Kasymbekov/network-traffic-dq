#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DB_SERVICE="db"
DB_USER="pipeline_user"
DB_NAME="network_traffic_smoke"

PYTHON="$PROJECT_ROOT/.venv/bin/python"
ENV_FILE="$PROJECT_ROOT/.env"
DATA_FILE="$PROJECT_ROOT/data/raw/UNSW_NB15_training-set.csv"

EXPECTED_RECORDS=175341

# Check required files
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: .env file not found." >&2
  exit 1
fi

if [[ ! -f "$DATA_FILE" ]]; then
  echo "Error: Training dataset not found" >&2
  exit 1
fi

if [[ ! -x "$PYTHON" ]]; then
  echo "Error: ETL virtual environment not found" >&2
  exit 1
fi

# Load environment variables.
set -a
source "$ENV_FILE"
set +a

: "${DATABASE_URL:?DATABASE_URL is not configured}"

# Constuct a connection string for the smoke-test database.
SMOKE_DATABASE_URL="$(
  "$PYTHON" -c '
import os
import sys
from psycopg.conninfo import make_conninfo

print(
  make_conninfo(
      os.environ["DATABASE_URL"],
      dbname=sys.argv[1],
    )
  )
' "$DB_NAME"
)"

# All Python pipeline modules will use the isolated database.
export DATABASE_URL="$SMOKE_DATABASE_URL"

echo "=== Initializing database ==="

"$PROJECT_ROOT/scripts/init_db.sh" "$DB_NAME"

echo "=== Loading raw data ==="

cd "$PROJECT_ROOT"
"$PYTHON" -m pipeline.load

echo "=== Transforming data ==="

"$PYTHON" -m pipeline.transform

echo "=== Generating quality reports ==="

"$PYTHON" -m pipeline.quality

echo "=== Building analytics MART ==="

"$PYTHON" -m pipeline.mart

echo "=== Verifying results ==="

SQL="
SELECT
  (SELECT COUNT(*) FROM raw.network_flows),
  (SELECT COUNT(*) FROM clean.network_flows),
  (SELECT COUNT(*) FROM clean.rejected_flows),
  (SELECT COALESCE(SUM(flow_count), 0)
   FROM mart.protocol_statistics),
  (SELECT COALESCE(SUM(flow_count), 0)
   FROM mart.attack_statistics),
  (SELECT COUNT(*) FROM dq.batch_metrics
   WHERE total_records = $EXPECTED_RECORDS
    AND valid_records = $EXPECTED_RECORDS
    AND rejected_records = 0
    AND unprocessed_records = 0
    AND conflicting_records = 0
    AND rejection_rate = 0);
"

RESULT=$(docker compose -f "$PROJECT_ROOT/compose.yaml" \
  exec -T "$DB_SERVICE" \
  psql -v ON_ERROR_STOP=1 -U "$DB_USER" -d "$DB_NAME" \
  -tA -F '|' -c "$SQL")

IFS='|' read -r raw clean rejected protocol attack reports <<< "$RESULT"

printf "RAW records:      %s\n" "$raw"
printf "CLEAN records:    %s\n" "$clean"
printf "REJECTED records: %s\n" "$rejected"
printf "Protocol flows:   %s\n" "$protocol"
printf "Attack flows:     %s\n" "$attack"
printf "Valid DQ reports: %s\n" "$reports"

if [[ "$raw" != "$EXPECTED_RECORDS" ||
      "$clean" != "$EXPECTED_RECORDS" ||
      "$rejected" != "0" ||
      "$protocol" != "$EXPECTED_RECORDS" ||
      "$attack" != "$EXPECTED_RECORDS" ||
      "$reports" != "1" ]]; then
  echo "Smoke test FAILED." >&2
  exit 1
fi

echo "Smoke test PASSED."
