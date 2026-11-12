import os
from decimal import Decimal

import psycopg


def get_batch_statistics(connection):
    query = """
        SELECT
            r.batch_id,
            MIN(r.source_file) AS source_file,
            COUNT(*) AS total_records,

            COUNT(c.raw_id) AS valid_records,
            COUNT(j.raw_id) AS rejected_records,

            COUNT(*) FILTER (
                WHERE c.raw_id IS NULL
                  AND j.raw_id IS NULL
            ) AS unprocessed_records,

            COUNT(*) FILTER (
                WHERE c.raw_id IS NOT NULL
                  AND j.raw_id IS NOT NULL
            ) AS conflicting_records

        FROM raw.network_flows AS r

        LEFT JOIN clean.network_flows AS c
            ON r.raw_id = c.raw_id

        LEFT JOIN clean.rejected_flows AS j
            ON r.raw_id = j.raw_id

        GROUP BY r.batch_id
        ORDER BY r.batch_id
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()


def get_rule_failures(connection, batch_id):
    query = """
        SELECT
            reasons.reason,
            COUNT(*) AS occurrences

        FROM clean.rejected_flows AS j

        JOIN raw.network_flows AS r
            ON j.raw_id = r.raw_id

        CROSS JOIN LATERAL
            jsonb_array_elements_text(
                j.rejection_reasons
            ) AS reasons(reason)

        WHERE r.batch_id = %s

        GROUP BY reasons.reason
        ORDER BY reasons.reason
    """

    with connection.cursor() as cursor:
        cursor.execute(query, (batch_id,))
        return cursor.fetchall()


def save_batch_metrics(connection, stats):
    (
        batch_id,
        source_file,
        total,
        valid,
        rejected,
        unprocessed,
        conflicting,
    ) = stats

    rejection_rate = (
        Decimal(rejected) * 100 / Decimal(total)
    ).quantize(Decimal("0.01"))

    query = """
        INSERT INTO dq.batch_metrics (
            batch_id,
            source_file,
            total_records,
            valid_records,
            rejected_records,
            unprocessed_records,
            conflicting_records,
            rejection_rate
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)

        ON CONFLICT (batch_id)
        DO UPDATE SET
            source_file = EXCLUDED.source_file,
            total_records = EXCLUDED.total_records,
            valid_records = EXCLUDED.valid_records,
            rejected_records = EXCLUDED.rejected_records,
            unprocessed_records = EXCLUDED.unprocessed_records,
            conflicting_records = EXCLUDED.conflicting_records,
            rejection_rate = EXCLUDED.rejection_rate,
            calculated_at = NOW()
    """

    with connection.cursor() as cursor:
        cursor.execute(
            query,
            (
                batch_id,
                source_file,
                total,
                valid,
                rejected,
                unprocessed,
                conflicting,
                rejection_rate,
            ),
        )


def save_rule_failures(connection, batch_id, failures):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            DELETE FROM dq.rule_failures
            WHERE batch_id = %s
            """,
            (batch_id,),
        )

        if failures:
            cursor.executemany(
                """
                INSERT INTO dq.rule_failures (
                    batch_id,
                    reason,
                    occurrences
                )
                VALUES (%s, %s, %s)
                """,
                [
                    (batch_id, reason, count)
                    for reason, count in failures
                ],
            )


def generate_reports(connection):
    batches = get_batch_statistics(connection)

    # Finish the read transaction before starting
    # individual report transactions.
    connection.commit()

    for stats in batches:
        batch_id = stats[0]

        with connection.transaction():
            save_batch_metrics(connection, stats)

            failures = get_rule_failures(
                connection,
                batch_id,
            )

            save_rule_failures(
                connection,
                batch_id,
                failures,
            )

        print(
            f"Reported batch {batch_id}: "
            f"{stats[2]} records, "
            f"{stats[4]} rejected"
        )

    return len(batches)


def main():
    connection_string = os.environ["DATABASE_URL"]

    with psycopg.connect(connection_string) as connection:
        total = generate_reports(connection)

    print(f"Generated reports for {total} batches.")


if __name__ == "__main__":
    main()
