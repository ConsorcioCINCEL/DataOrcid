"""Complete the tracking index missing from the upgrade path to release 2.1.

Run with ORCID_APP_CONFIG pointing to the deployment's TOML configuration.
This reconciles an existing database with the new TrackingLog model without
changing its records or adding a separate Alembic branch.
"""

import os

import toml
from sqlalchemy import create_engine, inspect, text


def main():
    config = toml.load(os.environ["ORCID_APP_CONFIG"])
    engine = create_engine(config["database"]["uri"])
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        connection.execute(text(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_tracking_logs_username "
            "ON tracking_logs (username)"
        ))
        index = next(
            item for item in inspect(connection).get_indexes("tracking_logs")
            if item["name"] == "ix_tracking_logs_username"
        )
        valid = connection.execute(text(
            "SELECT indisvalid FROM pg_index "
            "WHERE indexrelid = 'ix_tracking_logs_username'::regclass"
        )).scalar()
        if index["column_names"] != ["username"] or index["unique"] or not valid:
            raise RuntimeError("The tracking username index needs manual inspection.")
    engine.dispose()
    print("Tracking username index verified.")


if __name__ == "__main__":
    main()
