import os
from pathlib import Path

import psycopg

from pipeline.generate_broken import main as generate_broken
from pipeline.load import get_batch_id, load_csv
from pipeline.transform import transform_all

ROOT = Path(__file__).resolve().parent.parent
TEST_CSV = ROOT / "data/broken/unsw_corrupted.csv"

def test_corrupted_data_pipeline():
    connection_string = os.environ["TEST_DATABASE_URL"]

    params = psycopg.conninfo.conninfo_to_dict(connection_string)

    if params.get("dbname") != "network_traffic_test":
        raise RuntimeError("Refusing to modify a non-test database")

    generate_broken()

    batch_id = get_batch_id(TEST_CSV)

    with psycopg.connect(connection_string) as conn:

        with conn.cursor() as cursor:
            cursor.execute("""
                TRUNCATE TABLE
                    clean.network_flows,
                    clean.rejected_flows,
                    raw.network_flows
                RESTART IDENTITY
            """)

        conn.commit()

        # First execution
        load_csv(conn, TEST_CSV, batch_id)
        transform_all(conn)

        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM raw.network_flows"
            )
            assert cursor.fetchone()[0] == 8

            cursor.execute(
                "SELECT COUNT(*) FROM clean.network_flows"
            )
            assert cursor.fetchone()[0] == 1

            cursor.execute(
                "SELECT COUNT(*) FROM clean.rejected_flows"
            )
            assert cursor.fetchone()[0] == 7

            cursor.execute("""
                SELECT j.rejection_reasons
                FROM clean.rejected_flows AS j
                JOIN raw.network_flows AS r
                    ON r.raw_id = j.raw_id
                WHERE r.source_row_number = 8
            """)

            reasons = cursor.fetchone()[0]
            assert len(reasons) == 3

        # Second execution: idempotency check.
        load_csv(conn, TEST_CSV, batch_id)
        transform_all(conn)

        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    (SELECT COUNT(*) FROM raw.network_flows),
                    (SELECT COUNT(*) FROM clean.network_flows),
                    (SELECT COUNT(*) FROM clean.rejected_flows)
            """)

            raw_count, valid_count, rejected_count = cursor.fetchone()

            assert raw_count == 8
            assert valid_count == 1
            assert rejected_count == 7
