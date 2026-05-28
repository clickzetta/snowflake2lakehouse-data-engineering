"""
09_process_incrementally.py — Load new data and trigger pipeline update.

Migrated from: 01_snowflake/steps/09_process_incrementally.sql

Migration notes:
  Snowflake: COPY INTO ORDER_HEADER FROM @stage/year=2022 + EXECUTE TASK
  Lakehouse: Upload new Parquet files to Volume + run Python scripts directly
      - ALTER WAREHOUSE SIZE = XLARGE → removed; VCluster scales automatically
      - COPY INTO from stage → session.read.parquet(vol_path).copy_into_table()
      - EXECUTE TASK ORDERS_UPDATE_TASK → python 06_orders_update.py

  This script simulates adding year=2022 data as an incremental load.
  In production, new files would arrive in the Volume via Pipe or Studio Sync.

Run with:
  python steps/09_process_incrementally.py

Prerequisites:
  Download year=2022 order files from S3 first:
    aws s3 cp s3://sfquickstarts/data-engineering-with-snowpark-python/pos/order_header/year=2022/<file>.snappy.parquet \\
      ./datasets/pos/order_header/year=2022/ --no-sign-request
    aws s3 cp s3://sfquickstarts/data-engineering-with-snowpark-python/pos/order_detail/year=2022/<file>.snappy.parquet \\
      ./datasets/pos/order_detail/year=2022/ --no-sign-request
"""

import os
import pathlib
import importlib.util
import sys

from dotenv import load_dotenv
from clickzetta.zettapark.session import Session

load_dotenv(pathlib.Path(__file__).parent.parent / ".env")

VOL_SCHEMA = "public"   # Volume lives in public schema
VOL_NAME   = os.environ.get("CLICKZETTA_VOLUME", "frostbyte_vol")
VOL_BASE   = f"vol://{VOL_SCHEMA}.{VOL_NAME}"

INCREMENTAL_TABLES = {
    "order_header": f"{VOL_BASE}/pos/order_header/year=2022/",
    "order_detail": f"{VOL_BASE}/pos/order_detail/year=2022/",
}


def create_session():
    return Session.builder.configs({
        "username":  os.environ["CLICKZETTA_USERNAME"],
        "password":  os.environ["CLICKZETTA_PASSWORD"],
        "service":   os.environ["CLICKZETTA_SERVICE"],
        "instance":  os.environ["CLICKZETTA_INSTANCE"],
        "workspace": os.environ["CLICKZETTA_WORKSPACE"],
        "schema":    VOL_SCHEMA,
        "vcluster":  os.environ.get("CLICKZETTA_VCLUSTER", "default"),
    }).create()


def upload_incremental_files(session):
    """Upload year=2022 files from local datasets/ to Volume."""
    datasets = pathlib.Path(__file__).parent.parent / "datasets"
    vol_base = f"vol://{VOL_SCHEMA}.{VOL_NAME}"

    for year_dir in ["pos/order_header/year=2022", "pos/order_detail/year=2022"]:
        local_dir = datasets / year_dir
        if not local_dir.exists():
            print(f"  SKIP: {local_dir} not found (download year=2022 files first)")
            continue
        for f in sorted(local_dir.glob("*.parquet")):
            dest = f"{vol_base}/{year_dir}/{f.name}"
            print(f"  Uploading {f.name} → {dest}")
            session.file.put(str(f), dest, auto_compress=False, overwrite=True)


def load_incremental_tables(session):
    """Append year=2022 data into raw tables, then refresh pos_flattened_v_table."""
    for tname, vol_path in INCREMENTAL_TABLES.items():
        print(f"  Loading {tname} from {vol_path}")
        try:
            df = session.read.option("compression", "snappy").parquet(vol_path)
            # append mode: add new rows without overwriting existing year=2021 data
            df.write.save_as_table(f"frostbyte_raw_pos.{tname}", mode="append")
            count = session.table(f"frostbyte_raw_pos.{tname}").count()
            print(f"    → {count:,} total rows")
        except Exception as e:
            print(f"    SKIP: {e}")

    # Refresh pos_flattened_v_table so the stream picks up new rows
    print("  Refreshing pos_flattened_v_table...")
    session.sql("""
        INSERT INTO frostbyte_harmonized.pos_flattened_v_table
        SELECT * FROM frostbyte_harmonized.pos_flattened_v
        WHERE order_ts >= '2022-01-01'
    """).collect()
    new_count = session.table("frostbyte_harmonized.pos_flattened_v_table").count()
    print(f"    → pos_flattened_v_table: {new_count:,} total rows")


def run_pipeline(session):
    """Trigger orders_update and daily_city_metrics (equivalent to EXECUTE TASK)."""
    print("Running orders_update...")
    steps_dir = pathlib.Path(__file__).parent

    for script in ["06_orders_update.py", "07_daily_city_metrics.py"]:
        spec = importlib.util.spec_from_file_location("step", steps_dir / script)
        mod  = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.create_session = lambda: session
        if hasattr(mod, "merge_order_updates"):
            mod.merge_order_updates(session)
        if hasattr(mod, "merge_daily_city_metrics"):
            mod.merge_daily_city_metrics(session)


if __name__ == "__main__":
    session = create_session()
    print("Step 1: Upload incremental files to Volume...")
    upload_incremental_files(session)
    print("Step 2: Load incremental data into raw tables...")
    load_incremental_tables(session)
    print("Step 3: Run pipeline update...")
    run_pipeline(session)
    print("\nIncremental processing complete.")
