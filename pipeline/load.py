import csv
import hashlib
import os
import uuid

from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parent.parent

CSV_PATH = (
    ROOT / "data" / "raw" / "UNSW_NB15_training-set.csv"
)

BATCH_SIZE = 1000

def calculate_file_hash(path):
    sha256 = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()

def get_batch_id(path):
    file_hash = calculate_file_hash(path)

    return uuid.uuid5(uuid.NAMESPACE_URL, file_hash)

def load_csv(connection, path, batch_id):
    total_rows = 0

    query = """
        INSERT INTO raw.network_flows (
            batch_id,
            source_file,
            source_row_number,
            payload
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (batch_id, source_row_number)
        DO NOTHING
    """

    with path.open(
        newline="",
        encoding="utf-8-sig",
    ) as file:
        reader = csv.DictReader(file)

        batch = []

        for row_number, row in enumerate(reader, start=2):
            batch.append(
                (
                    batch_id,
                    path.name,
                    row_number,
                    Jsonb(row),
                )
            )

            if len(batch) >= BATCH_SIZE:
                with connection.cursor() as cursor:
                    cursor.executemany(query, batch)

                connection.commit()

                total_rows += len(batch)
                print(f"Processed: {total_rows}")

                batch.clear()

        if batch:
            with connection.cursor() as cursor:
                cursor.executemany(query, batch)

            connection.commit()
            total_rows += len(batch) 

    return total_rows

def main():
    batch_id = get_batch_id(CSV_PATH)

    print(f"Batch ID: {batch_id}")

    connection_string = os.environ["DATABASE_URL"]

    with psycopg.connect(connection_string) as connection:
        total = load_csv(
            connection,
            CSV_PATH,
            batch_id,
        )

    print(f"Finished processing {total} records.")

if __name__ == "__main__":
    main()
