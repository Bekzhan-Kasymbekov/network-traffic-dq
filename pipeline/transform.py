import os

import psycopg
from psycopg.types.json import Jsonb

from pipeline.validate import validate_flow

BATCH_SIZE = 1000

def transform_batch(connection, rows):
    valid_records = []
    rejected_records = []

    for raw_id, batch_id, payload in rows:
        record, errors = validate_flow(payload)

        if errors:
            rejected_records.append(
                (
                    raw_id,
                    batch_id,
                    Jsonb(errors),
                )
            )
        else:
            valid_records.append(
                (
                    raw_id,
                    batch_id,
                    record["flow_id"],
                    record["dur"],
                    record["proto"],
                    record["service"],
                    record["state"],
                    record["spkts"],
                    record["dpkts"],
                    record["sbytes"],
                    record["dbytes"],
                    record["attack_cat"],
                    record["label"],
                )
            )

    valid_query = """
        INSERT INTO clean.network_flows (
            raw_id, batch_id, flow_id, dur, proto,
            service, state, spkts, dpkts,
            sbytes, dbytes, attack_cat, label
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (raw_id) DO NOTHING
    """

    rejected_query = """
        INSERT INTO clean.rejected_flows (
            raw_id, batch_id, rejection_reasons
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (raw_id) DO NOTHING
    """

    with connection.cursor() as cursor:
        if valid_records:
            cursor.executemany(valid_query, valid_records)

        if rejected_records:
            cursor.executemany(rejected_query, rejected_records)
    
    return len(valid_records), len(rejected_records)

def transform_all(connection):
    last_raw_id = 0
    total_valid = 0
    total_rejected = 0

    while True:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT raw_id, batch_id, payload
                FROM raw.network_flows
                WHERE raw_id > %s
                ORDER BY raw_id
                LIMIT %s
                """,
                (last_raw_id, BATCH_SIZE)
            )
            
            rows = cursor.fetchall()

        if not rows:
            break

        valid_count, rejected_count = transform_batch(
            connection,
            rows,
        )

        connection.commit()

        last_raw_id = rows[-1][0]

        total_valid += valid_count
        total_rejected += rejected_count

        print(
            f"Processed: {total_valid + total_rejected} | "
            f"Valid: {total_valid} | "
            f"Rejected: {total_rejected}"
        )

    return total_valid, total_rejected

def main():
    connection_string = os.environ["DATABASE_URL"]

    with psycopg.connect(connection_string) as connection:
        valid, rejected = transform_all(connection)

    print(
        f"Transformation complete. "
        f"Valid: {valid}, Rejected: {rejected}"
    )

if __name__ == "__main__":
    main()
