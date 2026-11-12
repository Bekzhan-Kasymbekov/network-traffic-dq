import os

import psycopg

def build_protocol_statistics(connection):
    query = """
        INSERT INTO mart.protocol_statistics (
            batch_id,
            proto,
            label,
            flow_count,
            total_source_bytes,
            total_destination_bytes,
            avg_duration
        )
        SELECT
            batch_id,
            proto,
            label,
            COUNT(*),
            SUM(sbytes),
            SUM(dbytes),
            AVG(dur)
        FROM clean.network_flows
        GROUP BY batch_id, proto, label
    """

    with connection.cursor() as cursor:
        cursor.execute(query)

def build_attack_statistics(connection):
    query = """
        INSERT INTO mart.attack_statistics (
            batch_id,
            attack_cat,
            flow_count
        )
        SELECT
            batch_id,
            COALESCE(
                NULLIF(BTRIM(attack_cat), ''),
                'Unknown'
            ),
            COUNT (*)
        FROM clean.network_flows
        GROUP BY
            batch_id,
            COALESCE(
                NULLIF(BTRIM(attack_cat), ''),
                'Unknown'
            )
    """

    with connection.cursor() as cursor:
        cursor.execute(query)

def build_marts(connection):
    with connection.transaction():
        # Rebuild both marts from the current CLEAN data.
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM mart.protocol_statistics"
            )
            cursor.execute(
                "DELETE FROM mart.attack_statistics"
            )

        build_protocol_statistics(connection)
        build_attack_statistics(connection)

    print("Analytics marts rebuilt successfully.")

def main():
    connection_string = os.environ["DATABASE_URL"]

    with psycopg.connect(
        connection_string,
        autocommit=True,
    ) as connection:
        build_marts(connection)

if __name__ == "__main__":
    main()
