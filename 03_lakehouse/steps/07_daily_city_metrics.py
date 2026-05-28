"""
07_daily_city_metrics.py — Aggregate daily sales metrics by city.

Migrated from: 01_snowflake/steps/07_daily_city_metrics_update_sp/

Migration notes:
  Snowflake: Python Stored Procedure + FROSTBYTE_WEATHERSOURCE (paid data marketplace)
  Lakehouse: Plain Python script; weather join is SKIPPED (no equivalent dataset)
      - The weather data (FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.*) is a Snowflake
        Marketplace dataset not available in Lakehouse.
      - We compute daily_city_metrics without weather columns.
        Weather columns are kept in the schema as NULL for structural compatibility.
      - ALTER WAREHOUSE SIZE = XLARGE → removed; VCluster scales automatically
      - F.call_builtin("ZEROIFNULL", ...) → F.coalesce(..., F.lit(0))
      - F.call_udf("ANALYTICS.FAHRENHEIT_TO_CELSIUS_UDF", ...) → direct SQL UDF call

  snowflake.snowpark → clickzetta.zettapark  (import path only)

Run with:
  python steps/07_daily_city_metrics.py
"""

import os
import pathlib

from dotenv import load_dotenv
from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
import clickzetta.zettapark.types as T

load_dotenv(pathlib.Path(__file__).parent.parent / ".env")


def create_session():
    return Session.builder.configs({
        "username":  os.environ["CLICKZETTA_USERNAME"],
        "password":  os.environ["CLICKZETTA_PASSWORD"],
        "service":   os.environ["CLICKZETTA_SERVICE"],
        "instance":  os.environ["CLICKZETTA_INSTANCE"],
        "workspace": os.environ["CLICKZETTA_WORKSPACE"],
        "schema":    "frostbyte_analytics",
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


def create_daily_city_metrics_table(session):
    schema = T.StructType([
        T.StructField("date",                        T.DateType()),
        T.StructField("city_name",                   T.StringType()),
        T.StructField("country_desc",                T.StringType()),
        T.StructField("daily_sales",                 T.DecimalType(18, 2)),
        T.StructField("avg_temperature_fahrenheit",  T.DecimalType(18, 4)),
        T.StructField("avg_temperature_celsius",     T.DecimalType(18, 4)),
        T.StructField("avg_precipitation_inches",    T.DecimalType(18, 4)),
        T.StructField("avg_precipitation_millimeters", T.DecimalType(18, 4)),
        T.StructField("max_wind_speed_100m_mph",     T.DecimalType(18, 4)),
        T.StructField("meta_updated_at",             T.TimestampType()),
    ])
    session.create_dataframe([[None] * len(schema.names)], schema=schema) \
           .na.drop() \
           .write.mode("overwrite").save_as_table("frostbyte_analytics.daily_city_metrics")
    print("  Table frostbyte_analytics.daily_city_metrics created.")


def merge_daily_city_metrics(session):
    stream_count = session.table("frostbyte_harmonized.orders_stream").count()
    print(f"  {stream_count:,} records in orders_stream")
    if stream_count == 0:
        print("  No new data in stream, skipping merge.")
        return

    # Aggregate daily sales by city from stream
    # Snowflake used F.call_builtin("ZEROIFNULL", ...) → F.coalesce(..., F.lit(0))
    orders = (
        session.table("frostbyte_harmonized.orders_stream")
        .group_by(F.col("order_ts_date"), F.col("primary_city"), F.col("country"))
        .agg(F.sum(F.col("price")).alias("price_sum"))
        .with_column("daily_sales", F.coalesce(F.col("price_sum"), F.lit(0)))
        .select(
            F.col("order_ts_date").alias("date"),
            F.col("primary_city").alias("city_name"),
            F.col("country").alias("country_desc"),
            F.col("daily_sales"),
        )
    )

    # Weather columns: NULL (FROSTBYTE_WEATHERSOURCE not available in Lakehouse)
    daily_city_metrics_stg = orders.with_column("avg_temperature_fahrenheit",  F.lit(None).cast(T.DecimalType(18, 4))) \
                                   .with_column("avg_temperature_celsius",     F.lit(None).cast(T.DecimalType(18, 4))) \
                                   .with_column("avg_precipitation_inches",    F.lit(None).cast(T.DecimalType(18, 4))) \
                                   .with_column("avg_precipitation_millimeters", F.lit(None).cast(T.DecimalType(18, 4))) \
                                   .with_column("max_wind_speed_100m_mph",     F.lit(None).cast(T.DecimalType(18, 4)))

    target = session.table("frostbyte_analytics.daily_city_metrics")
    updates = {c: daily_city_metrics_stg[c] for c in daily_city_metrics_stg.schema.names}
    updates["meta_updated_at"] = F.current_timestamp()

    target.merge(
        daily_city_metrics_stg,
        (target["date"] == daily_city_metrics_stg["date"]) &
        (target["city_name"] == daily_city_metrics_stg["city_name"]) &
        (target["country_desc"] == daily_city_metrics_stg["country_desc"]),
        [F.when_matched().update(updates),
         F.when_not_matched().insert(updates)]
    )
    count = session.table("frostbyte_analytics.daily_city_metrics").count()
    print(f"  Merge complete. daily_city_metrics now has {count:,} rows.")


if __name__ == "__main__":
    session = create_session()
    if not table_exists(session, "frostbyte_analytics", "daily_city_metrics"):
        print("Creating daily_city_metrics table...")
        create_daily_city_metrics_table(session)
    print("Merging daily city metrics...")
    merge_daily_city_metrics(session)
    print("\nDone. Run next step:")
    print("  bash steps/08_orchestrate_tasks.sh")
