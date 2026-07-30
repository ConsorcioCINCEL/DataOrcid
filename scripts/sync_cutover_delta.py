#!/usr/bin/env python3
"""Synchronize production users and tracking history before final cutover.

This deliberately copies only mutable account records and new tracking logs.
The refreshed ORCID/OpenAlex caches in the prepared PostgreSQL database remain
untouched.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from migrate_mysql_to_postgresql import (
    EXCLUDED_INSTITUTIONS,
    EXCLUDED_RORS,
    connect_mysql,
    connect_postgresql,
    mysql_columns,
    normalized_filter,
    postgresql_columns,
    quote_mysql,
    quote_postgresql,
    read_database_uri,
)


SYNC_TABLES = ("user", "tracking_logs")
USER_BOOLEAN_COLUMNS = ("is_admin", "is_manager")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-config",
        type=Path,
        default=Path("/var/www/orcid-cincel/config/config.toml"),
    )
    parser.add_argument(
        "--target-config",
        type=Path,
        default=Path("/var/www/DataOrcid/config/config.toml"),
    )
    parser.add_argument("--target-database", default="orcid_cincel_prod")
    parser.add_argument(
        "--confirm",
        required=True,
        help="Must exactly match --target-database.",
    )
    parser.add_argument("--batch-size", type=int, default=2000)
    return parser.parse_args()


def excluded_user_ids(mysql_connection) -> list[int]:
    ror_placeholders = ", ".join(["%s"] * len(EXCLUDED_RORS))
    institution_placeholders = ", ".join(
        ["%s"] * len(EXCLUDED_INSTITUTIONS)
    )
    query = f"""
        SELECT id
          FROM user
         WHERE COALESCE(ror_id, '') IN ({ror_placeholders})
            OR COALESCE(institution_name, '') IN ({institution_placeholders})
    """
    with mysql_connection.cursor() as cursor:
        cursor.execute(query, (*EXCLUDED_RORS, *EXCLUDED_INSTITUTIONS))
        return [int(row[0]) for row in cursor.fetchall()]


def upsert_table(
    mysql_connection,
    pg_connection,
    table: str,
    batch_size: int,
) -> int:
    source_columns = mysql_columns(mysql_connection, table)
    destination_columns = postgresql_columns(pg_connection, table)
    columns = [
        column for column in source_columns if column in destination_columns
    ]
    quoted_columns = ", ".join(
        quote_postgresql(column) for column in columns
    )
    placeholders = ", ".join(["%s"] * len(columns))

    if table == "user":
        update_columns = [column for column in columns if column != "id"]
        conflict_action = "DO UPDATE SET " + ", ".join(
            f"{quote_postgresql(column)} = EXCLUDED.{quote_postgresql(column)}"
            for column in update_columns
        )
    else:
        conflict_action = "DO NOTHING"

    insert_sql = (
        f"INSERT INTO {quote_postgresql(table)} ({quoted_columns}) "
        f"VALUES ({placeholders}) ON CONFLICT (id) {conflict_action}"
    )
    select_sql = (
        "SELECT "
        + ", ".join(quote_mysql(column) for column in columns)
        + f" FROM {quote_mysql(table)} {normalized_filter(table)}"
    )

    count = 0
    with mysql_connection.cursor() as source_cursor:
        source_cursor.execute(select_sql)
        with pg_connection.cursor() as target_cursor:
            while True:
                rows = source_cursor.fetchmany(batch_size)
                if not rows:
                    break
                if table == "user":
                    boolean_positions = [
                        columns.index(column)
                        for column in USER_BOOLEAN_COLUMNS
                        if column in columns
                    ]
                    normalized_rows = []
                    for row in rows:
                        values = list(row)
                        for position in boolean_positions:
                            if values[position] is not None:
                                values[position] = bool(values[position])
                        normalized_rows.append(tuple(values))
                    rows = normalized_rows
                target_cursor.executemany(insert_sql, rows)
                count += len(rows)
    return count


def remove_excluded_accounts(
    pg_connection,
    excluded_ids: list[int],
) -> tuple[int, int]:
    if not excluded_ids:
        return 0, 0
    with pg_connection.cursor() as cursor:
        cursor.execute(
            "DELETE FROM tracking_logs WHERE user_id = ANY(%s)",
            (excluded_ids,),
        )
        deleted_logs = cursor.rowcount
        cursor.execute(
            'DELETE FROM "user" WHERE id = ANY(%s)',
            (excluded_ids,),
        )
        deleted_users = cursor.rowcount
    return deleted_users, deleted_logs


def reset_sequences(pg_connection) -> None:
    with pg_connection.cursor() as cursor:
        for table in SYNC_TABLES:
            cursor.execute(
                "SELECT pg_get_serial_sequence(%s, 'id')",
                (table,),
            )
            sequence = cursor.fetchone()[0]
            if not sequence:
                continue
            cursor.execute(
                f"SELECT MAX(id) FROM {quote_postgresql(table)}"
            )
            maximum = cursor.fetchone()[0]
            cursor.execute(
                "SELECT setval(%s, %s, %s)",
                (sequence, maximum or 1, maximum is not None),
            )


def main() -> int:
    args = parse_args()
    if args.confirm != args.target_database:
        print(
            "Refusing synchronization: --confirm does not match target.",
            file=sys.stderr,
        )
        return 2
    if args.target_database in {"postgres", "dataorcid", "orcid_cincel"}:
        print("Refusing synchronization: protected database.", file=sys.stderr)
        return 2

    source_uri = read_database_uri(args.source_config)
    target_uri = read_database_uri(args.target_config)
    mysql_connection = connect_mysql(source_uri)
    pg_connection = connect_postgresql(target_uri, args.target_database)

    try:
        excluded_ids = excluded_user_ids(mysql_connection)
        users_seen = upsert_table(
            mysql_connection,
            pg_connection,
            "user",
            args.batch_size,
        )
        logs_seen = upsert_table(
            mysql_connection,
            pg_connection,
            "tracking_logs",
            args.batch_size,
        )
        users_deleted, logs_deleted = remove_excluded_accounts(
            pg_connection,
            excluded_ids,
        )
        reset_sequences(pg_connection)
        pg_connection.commit()

        print(f"Retained users synchronized: {users_seen}")
        print(f"Tracking rows inspected: {logs_seen}")
        print(f"Excluded users removed: {users_deleted}")
        print(f"Excluded tracking rows removed: {logs_deleted}")
        return 0
    except Exception:
        pg_connection.rollback()
        raise
    finally:
        mysql_connection.close()
        pg_connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
