"""
06_orders_update.py — Merge new orders from stream into ORDERS table.

Migrated from: 01_snowflake/steps/06_orders_update_sp/orders_update_sp/procedure.py

Migration notes:
  Snowflake: Python Stored Procedure (deployed to Snowflake, called via CALL)
  Lakehouse: Plain Python script (run directly or via cz-cli task)
      - No CREATE PROCEDURE / CALL needed
      - Logic is identical: read stream → merge into target table
      - ALTER WAREHOUSE SIZE = XLARGE → removed; VCluster scales automatically

  snowflake.snowpark → clickzetta.zettapark  (import path only)
  session.table().merge() → identical API

Run with:
  python steps/06_orders_update.py

Or schedule via cz-cli task (see 08_orchestrate_tasks.sh).
"""

import os
import pathlib

from dotenv import load_dotenv
from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F

load_dotenv(pathlib.Path(__file__).parent.parent / ".env")


def create_session():
    return Session.builder.configs({
        "username":  os.environ["CLICKZETTA_USERNAME"],
        "password":  os.environ["CLICKZETTA_PASSWORD"],
        "service":   os.environ["CLICKZETTA_SERVICE"],
        "instance":  os.environ["CLICKZETTA_INSTANCE"],
        "workspace": os.environ["CLICKZETTA_WORKSPACE"],
        "schema":    "frostbyte_harmonized",
        "vcluster":  os.environ.get("CLICKZETTA_VCLUSTER", "default"),
    }).create()


def table_exists(session, schema, name):
    result = session.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM information_schema.tables
        WHERE table_schema = '{schema.upper()}'
          AND table_name   = '{name.upper()}'
    """).collect()
    return result[0]["cnt"] > 0


def create_orders_table(session):
    # Create orders table with same schema as the view (not the materialized table
    # which has rsuffix-mangled column names from ZettaPark DataFrame joins)
    session.sql("""
        CREATE TABLE IF NOT EXISTS frostbyte_harmonized.orders
        AS SELECT *, CAST(NULL AS TIMESTAMP) AS meta_updated_at
        FROM frostbyte_harmonized.pos_flattened_v
        WHERE 1=0
    """).collect()
    print("  Table frostbyte_harmonized.orders created.")


def create_orders_stream(session):
    session.sql("""
        CREATE TABLE STREAM IF NOT EXISTS frostbyte_harmonized.orders_stream
        ON TABLE frostbyte_harmonized.orders
        WITH PROPERTIES ('TABLE_STREAM_MODE' = 'STANDARD')
    """).collect()
    print("  Stream frostbyte_harmonized.orders_stream created.")


def merge_order_updates(session):
    stream_count = session.table("frostbyte_harmonized.pos_flattened_v_stream").count()
    print(f"  {stream_count:,} records in pos_flattened_v_stream")

    orders_count = session.table("frostbyte_harmonized.orders").count()

    if stream_count == 0 and orders_count == 0:
        # Initial load: stream is empty because pos_flattened_v_table was just created.
        # Load directly from the view for the first run.
        print("  Initial load: reading from pos_flattened_v directly...")
        source = session.table("frostbyte_harmonized.pos_flattened_v")
    elif stream_count == 0:
        print("  No new data in stream, skipping merge.")
        return
    else:
        source = session.table("frostbyte_harmonized.pos_flattened_v_stream")

    target = session.table("frostbyte_harmonized.orders")
    cols_to_update = {c: source[c] for c in source.schema.names if "METADATA" not in c}
    updates = {**cols_to_update, "meta_updated_at": F.current_timestamp()}

    target.merge(
        source,
        target["order_detail_id"] == source["order_detail_id"],
        [F.when_matched().update(updates),
         F.when_not_matched().insert(updates)]
    )
    print(f"  Merge complete. orders table now has {target.count():,} rows.")


if __name__ == "__main__":
    session = create_session()
    if not table_exists(session, "frostbyte_harmonized", "orders"):
        print("Creating orders table and stream...")
        create_orders_table(session)
        create_orders_stream(session)
    print("Merging order updates...")
    merge_order_updates(session)
    print("\nDone. Run next step:")
    print("  python steps/07_daily_city_metrics.py")
