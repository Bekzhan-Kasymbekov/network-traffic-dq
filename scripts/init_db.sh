#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SQL_DIR="$PROJECT_ROOT/sql"


DB_SERVICE="db"
DB_USER="pipeline_user"
DB_NAME="${1:-network_traffic_fresh}"

if [[ ! "$DB_NAME" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
  echo "Error: Invalid databse name: $DB_NAME" >&2
  exit 1
fi

# Make sure the PostgreSQL container is running.
if ! docker compose -f "$PROJECT_ROOT/compose.yaml" ps --status running --services |
  grep -qx "$DB_SERVICE"; then
  echo "Error: PostgreSQL container is not running." >&2
  exit 1
fi

# Check whether the database already exists.
db_exists=$(docker compose -f "$PROJECT_ROOT/compose.yaml" exec -T "$DB_SERVICE" \
  psql -U "$DB_USER" -d postgres -tAc \
  "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'")

if [[ "$db_exists" != "1" ]]; then
  echo "Creating database: $DB_NAME"

  docker compose -f "$PROJECT_ROOT/compose.yaml" exec -T "$DB_SERVICE" \
    psql -v ON_ERROR_STOP=1 -U "$DB_USER" -d postgres \
    -c "CREATE DATABASE \"$DB_NAME\";"
else
  echo "Database already exists: $DB_NAME"
fi

# Check if SQL files exist.
shopt -s nullglob

migration_files=("$SQL_DIR"/0*.sql)

if (( ${#migration_files[@]} == 0 )); then
  echo "Error: No SQL migration files found." >&2
  exit 1
fi

# Apply SQL migrations in oder.
for file in "$SQL_DIR"/0*.sql; do
  echo "Applying migration: $file"

  docker compose -f "$PROJECT_ROOT/compose.yaml" exec -T "$DB_SERVICE" \
    psql -v ON_ERROR_STOP=1 -U "$DB_USER" -d "$DB_NAME" < "$file"
done

echo "Database initialization completed successfully."
