"""
04_create_pos_view.py — Create POS flattened view and Table Stream.

Migrated from: 01_snowflake/steps/04_create_pos_view.py

Migration notes:
  snowflake.snowpark.Session  → clickzetta.zettapark.session.Session
  snowflake.snowpark.functions → clickzetta.zettapark.functions
  session.use_schema()        → identical
  CREATE STREAM ON VIEW       → CREATE TABLE STREAM ON VIEW (same syntax)
  SYSTEM$STREAM_HAS_DATA()    → not needed here; used in task orchestration

Run with:
  python steps/04_create_pos_view.py
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


def create_pos_view(session):
    """
    Flatten order_header + order_detail + dimension tables into a single view.
    Column names are based on actual Parquet schema from S3.
    Note: some column names differ from the original Snowflake project due to
    Parquet schema differences (e.g. location.city vs primary_city).
    """
    order_detail = session.table("frostbyte_raw_pos.order_detail")
    order_header = session.table("frostbyte_raw_pos.order_header")
    truck        = session.table("frostbyte_raw_pos.truck")
    menu         = session.table("frostbyte_raw_pos.menu")
    franchise    = session.table("frostbyte_raw_pos.franchise")
    location     = session.table("frostbyte_raw_pos.location")

    # Build the flattened SELECT as SQL and create a persistent view
    session.sql("""
        CREATE OR REPLACE VIEW frostbyte_harmonized.pos_flattened_v AS
        SELECT
            od.order_detail_id,
            od.order_id,
            oh.truck_id,
            od.menu_item_id,
            od.line_number,
            oh.order_ts,
            od.quantity,
            od.unit_price,
            od.price,
            oh.order_amount,
            oh.order_tax_amount,
            oh.order_discount_amount,
            oh.order_total,
            oh.location_id,
            l.city            AS primary_city,
            l.region,
            l.iso_country_code,
            l.country,
            t.truck_id        AS truck_id_t,
            t.menu_type_id,
            t.primary_city    AS truck_city,
            m.truck_brand_name,
            m.menu_type,
            m.menu_item_name,
            m.item_category,
            m.item_subcategory,
            m.cost_of_goods_usd,
            m.sale_price_usd,
            f.franchise_id,
            t.franchise_flag,
            f.first_name      AS franchisee_first_name,
            f.last_name       AS franchisee_last_name,
            DATE(oh.order_ts) AS order_ts_date
        FROM frostbyte_raw_pos.order_detail  od
        JOIN frostbyte_raw_pos.order_header  oh ON od.order_id      = oh.order_id
        JOIN frostbyte_raw_pos.truck         t  ON oh.truck_id      = t.truck_id
        JOIN frostbyte_raw_pos.franchise     f  ON t.franchise_id   = f.franchise_id
        JOIN frostbyte_raw_pos.location      l  ON oh.location_id   = l.location_id
        JOIN frostbyte_raw_pos.menu          m  ON od.menu_item_id  = m.menu_item_id
    """).collect()
    print("  View frostbyte_harmonized.pos_flattened_v created.")


def create_pos_view_stream(session):
    """
    Create a Table Stream for incremental CDC processing.

    Snowflake: CREATE STREAM ... ON VIEW pos_flattened_v
               (Snowflake supports streams directly on views)
    Lakehouse: TABLE STREAM only supports ON TABLE, not ON VIEW.
               Workaround: materialize the view into a table first,
               then create the stream on the table.
    """
    # Materialize view into a table so stream can be created on it
    session.sql("""
        CREATE TABLE IF NOT EXISTS frostbyte_harmonized.pos_flattened_v_table
        AS SELECT * FROM frostbyte_harmonized.pos_flattened_v
    """).collect()
    print("  Materialized view into frostbyte_harmonized.pos_flattened_v_table.")

    session.sql("""
        CREATE TABLE STREAM IF NOT EXISTS frostbyte_harmonized.pos_flattened_v_stream
        ON TABLE frostbyte_harmonized.pos_flattened_v_table
        WITH PROPERTIES ('TABLE_STREAM_MODE' = 'STANDARD')
    """).collect()
    print("  Stream frostbyte_harmonized.pos_flattened_v_stream created.")


def test_pos_view(session):
    df = session.table("frostbyte_harmonized.pos_flattened_v")
    print(f"  pos_flattened_v: {df.count():,} rows")
    df.limit(3).show()


if __name__ == "__main__":
    session = create_session()
    print("Creating POS flattened view...")
    create_pos_view(session)
    print("Creating stream on view...")
    create_pos_view_stream(session)
    print("Testing view...")
    test_pos_view(session)
    print("\nDone. Run next step:")
    print("  python steps/06_orders_update.py")
