#!/usr/bin/env python3
"""Migrate the current ORCID production snapshot into a prepared PostgreSQL DB.

The destination is expected to be a clone of the OpenAlex-enabled DataOrcid
database.  Legacy/cache tables are replaced from MySQL while the reusable
OpenAlex cache tables are retained.  Derived tables are deliberately cleared
and must be rebuilt after the import.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path
from urllib.parse import unquote, urlparse

import psycopg
import pymysql
from pymysql.cursors import SSCursor


DOI_PATTERN = re.compile(
    r"10\.\d{4,9}/[-._;()/:A-Z0-9]+",
    re.IGNORECASE,
)
MAX_DOI_LENGTH = 255

EXCLUDED_RORS = ("003hb2249", "03dmz0111", "048nfjm95")
EXCLUDED_INSTITUTIONS = (
    "Health Research Board",
    "Makerere University",
    "Maynooth University",
    "Universidad de Maynooth",
)

SOURCE_TABLES = (
    "user",
    "institution_registry",
    "institution_identifier",
    "institution_researcher",
    "researcher_status",
    "researcher_cache",
    "work_cache",
    "work_cache_run",
    "funding_cache",
    "funding_cache_run",
    "tracking_logs",
)

TRUNCATE_TABLES = (
    "sync_job_step",
    "sync_job",
    "duplicate_profile_review",
    "duplicate_profile_cache",
    "researcher_affiliation_evidence",
    "work_record_link",
    "canonical_work",
    "openalex_institution_work_fact",
    "institution_researcher",
    "institution_identifier",
    "institution_registry",
    "tracking_logs",
    "funding_cache_run",
    "funding_cache",
    "work_cache_run",
    "work_cache",
    "researcher_status",
    "researcher_cache",
    "orcid_cache",
    "user",
)

SOURCE_FILTERS = {
    "user": """
        WHERE COALESCE(ror_id, '') NOT IN ({rors})
          AND COALESCE(institution_name, '') NOT IN ({institutions})
    """,
    "institution_registry": "WHERE ror_id NOT IN ({rors})",
    "institution_identifier": """
        WHERE institution_id NOT IN (
            SELECT id FROM institution_registry WHERE ror_id IN ({rors})
        )
    """,
    "institution_researcher": """
        WHERE institution_id NOT IN (
            SELECT id FROM institution_registry WHERE ror_id IN ({rors})
        )
    """,
    "researcher_status": "WHERE ror_id NOT IN ({rors})",
    # Imported in full, then pruned efficiently in PostgreSQL after all retained
    # association/cache tables are present.  The corresponding MySQL columns
    # use incompatible legacy collations, making an anti-join prohibitively slow.
    "researcher_cache": "",
    "work_cache": "WHERE ror_id NOT IN ({rors})",
    "work_cache_run": "WHERE ror_id NOT IN ({rors})",
    "funding_cache": "WHERE ror_id NOT IN ({rors})",
    "funding_cache_run": "WHERE ror_id NOT IN ({rors})",
    "tracking_logs": """
        WHERE COALESCE(user_id, -1) NOT IN (
            SELECT id FROM user
             WHERE COALESCE(ror_id, '') IN ({rors})
                OR COALESCE(institution_name, '') IN ({institutions})
        )
    """,
}


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


def read_database_uri(path: Path) -> str:
    with path.open("rb") as handle:
        return tomllib.load(handle)["database"]["uri"]


def connect_mysql(uri: str):
    parsed = urlparse(uri.replace("mysql+pymysql://", "mysql://", 1))
    return pymysql.connect(
        host=parsed.hostname or "localhost",
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        charset="utf8mb4",
        cursorclass=SSCursor,
        autocommit=False,
    )


def connect_postgresql(uri: str, database: str):
    parsed = urlparse(uri.replace("postgresql+psycopg://", "postgresql://", 1))
    return psycopg.connect(
        host=parsed.hostname or "127.0.0.1",
        port=parsed.port or 5432,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        dbname=database,
        autocommit=False,
    )


def sql_literals(values: tuple[str, ...]) -> str:
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def normalized_filter(table: str) -> str:
    template = SOURCE_FILTERS.get(table, "")
    return template.format(
        rors=sql_literals(EXCLUDED_RORS),
        institutions=sql_literals(EXCLUDED_INSTITUTIONS),
    )


def mysql_columns(connection, table: str) -> list[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = DATABASE()
               AND table_name = %s
             ORDER BY ordinal_position
            """,
            (table,),
        )
        return [row[0] for row in cursor.fetchall()]


def postgresql_columns(connection, table: str) -> list[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name = %s
             ORDER BY ordinal_position
            """,
            (table,),
        )
        return [row[0] for row in cursor.fetchall()]


def normalize_doi(value) -> str | None:
    if value is None:
        return None
    match = DOI_PATTERN.search(str(value).strip())
    if not match:
        return None
    doi = match.group(0).lower().rstrip(".,;:")
    while doi.endswith(")") and doi.count(")") > doi.count("("):
        doi = doi[:-1]
    return doi if 0 < len(doi) <= MAX_DOI_LENGTH else None


def transform_row(table: str, columns: list[str], row: tuple) -> tuple:
    values = list(row)
    if table == "work_cache":
        doi_position = columns.index("doi")
        values.append(normalize_doi(values[doi_position]))
    elif table == "institution_researcher":
        flags = dict(zip(columns, values))
        evidence_sources = [
            scheme
            for scheme, field in (
                ("ror", "matched_by_ror"),
                ("grid", "matched_by_grid"),
                ("ringgold", "matched_by_ringgold"),
            )
            if flags.get(field)
        ]
        values.extend(("verified_search", json.dumps(evidence_sources), True))
    return tuple(values)


def target_columns_for(table: str, common_columns: list[str]) -> list[str]:
    columns = list(common_columns)
    if table == "work_cache":
        columns.append("doi_normalized")
    elif table == "institution_researcher":
        columns.extend(("evidence_type", "evidence_sources", "is_verified"))
    return columns


def quote_mysql(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def quote_postgresql(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def copy_table(mysql_connection, pg_connection, table: str, batch_size: int) -> int:
    source_columns = mysql_columns(mysql_connection, table)
    destination_columns = postgresql_columns(pg_connection, table)
    common_columns = [column for column in source_columns if column in destination_columns]
    copy_columns = target_columns_for(table, common_columns)

    select_columns = ", ".join(quote_mysql(column) for column in common_columns)
    source_sql = (
        f"SELECT {select_columns} FROM {quote_mysql(table)} "
        f"{normalized_filter(table)}"
    )
    copy_sql = (
        f"COPY {quote_postgresql(table)} "
        f"({', '.join(quote_postgresql(column) for column in copy_columns)}) "
        "FROM STDIN"
    )

    count = 0
    with mysql_connection.cursor() as source_cursor:
        source_cursor.execute(source_sql)
        with pg_connection.cursor() as target_cursor:
            with target_cursor.copy(copy_sql) as copy:
                while True:
                    rows = source_cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    for row in rows:
                        copy.write_row(transform_row(table, common_columns, row))
                    count += len(rows)
    return count


def prepare_destination(connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TEMP TABLE migration_excluded_openalex_keys
            ON COMMIT DROP AS
            SELECT DISTINCT doi_normalized
              FROM work_cache
             WHERE ror_id = ANY(%s)
               AND doi_normalized IS NOT NULL
            EXCEPT
            SELECT DISTINCT doi_normalized
              FROM work_cache
             WHERE NOT (ror_id = ANY(%s))
               AND doi_normalized IS NOT NULL
            """,
            (list(EXCLUDED_RORS), list(EXCLUDED_RORS)),
        )
        cursor.execute(
            "TRUNCATE "
            + ", ".join(quote_postgresql(table) for table in TRUNCATE_TABLES)
            + " RESTART IDENTITY CASCADE"
        )


def purge_excluded_openalex_data(connection) -> dict[str, int]:
    deleted: dict[str, int] = {}
    with connection.cursor() as cursor:
        cursor.execute(
            "DELETE FROM openalex_sync_run WHERE ror_id = ANY(%s)",
            (list(EXCLUDED_RORS),),
        )
        deleted["openalex_sync_run"] = cursor.rowcount

        for table in (
            "openalex_work_author",
            "openalex_work_institution",
            "openalex_work_metadata",
            "openalex_work_raw_cache",
        ):
            cursor.execute(
                f"""
                DELETE FROM {quote_postgresql(table)} cached
                 WHERE cached.doi_normalized IN (
                       SELECT doi_normalized
                         FROM migration_excluded_openalex_keys
                 )
                   AND NOT EXISTS (
                       SELECT 1
                         FROM work_cache retained
                        WHERE retained.doi_normalized = cached.doi_normalized
                   )
                """
            )
            deleted[table] = cursor.rowcount
    return deleted


def purge_unassociated_researchers(connection) -> int:
    """Remove profiles that only belonged to the excluded institutions."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            DELETE FROM researcher_cache cached
             WHERE NOT EXISTS (
                       SELECT 1 FROM researcher_status retained
                        WHERE retained.orcid = cached.orcid
                   )
               AND NOT EXISTS (
                       SELECT 1 FROM institution_researcher retained
                        WHERE retained.orcid = cached.orcid
                   )
               AND NOT EXISTS (
                       SELECT 1 FROM work_cache retained
                        WHERE retained.orcid = cached.orcid
                   )
               AND NOT EXISTS (
                       SELECT 1 FROM funding_cache retained
                        WHERE retained.orcid = cached.orcid
                   )
            """
        )
        return cursor.rowcount


def reset_sequences(connection) -> None:
    with connection.cursor() as cursor:
        for table in SOURCE_TABLES:
            if table == "researcher_cache":
                continue
            cursor.execute(
                "SELECT pg_get_serial_sequence(%s, 'id')",
                (table,),
            )
            sequence_name = cursor.fetchone()[0]
            if not sequence_name:
                continue
            cursor.execute(
                f"SELECT MAX(id) FROM {quote_postgresql(table)}"
            )
            max_id = cursor.fetchone()[0]
            cursor.execute(
                "SELECT setval(%s, %s, %s)",
                (sequence_name, max_id or 1, max_id is not None),
            )


def main() -> int:
    args = parse_args()
    if args.confirm != args.target_database:
        print("Refusing migration: --confirm does not match the target database.", file=sys.stderr)
        return 2
    if args.target_database in {"postgres", "dataorcid", "orcid_cincel"}:
        print("Refusing migration: protected database name.", file=sys.stderr)
        return 2

    source_uri = read_database_uri(args.source_config)
    target_uri = read_database_uri(args.target_config)
    counts: dict[str, int] = {}

    mysql_connection = connect_mysql(source_uri)
    pg_connection = connect_postgresql(target_uri, args.target_database)
    try:
        with pg_connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            current_database = cursor.fetchone()[0]
        if current_database != args.target_database:
            raise RuntimeError("Connected PostgreSQL database does not match requested target.")

        prepare_destination(pg_connection)
        for table in SOURCE_TABLES:
            counts[table] = copy_table(
                mysql_connection,
                pg_connection,
                table,
                args.batch_size,
            )
            print(f"{table}: {counts[table]} rows")

        removed_researchers = purge_unassociated_researchers(pg_connection)
        deleted = purge_excluded_openalex_data(pg_connection)
        reset_sequences(pg_connection)
        pg_connection.commit()

        with pg_connection.cursor() as cursor:
            cursor.execute("ANALYZE")
        pg_connection.commit()

        print("OpenAlex rows removed as exclusively excluded:", json.dumps(deleted, sort_keys=True))
        print(f"Unassociated researcher profiles removed: {removed_researchers}")
        print("Migration committed:", json.dumps(counts, sort_keys=True))
        return 0
    except Exception:
        pg_connection.rollback()
        raise
    finally:
        mysql_connection.close()
        pg_connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
