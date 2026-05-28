"""
02_load_raw.py — Load raw Parquet files from Volume into Lakehouse tables.

Migrated from: 01_snowflake/steps/02_load_raw.py

Migration notes:
  Snowflake: session.read.parquet("@external.frostbyte_raw_stage/pos/menu/")
             .copy_into_table("RAW_POS.MENU")
  Lakehouse: session.read.option("compression","snappy")
             .parquet("vol://frostbyte_raw_pos.frostbyte_vol/pos/menu/")
             .copy_into_table("frostbyte_raw_pos.menu")

  Key differences:
  - Stage path (@schema.stage/dir/) → Volume path (vol://schema.vol/dir/)
  - Schema names: RAW_POS → frostbyte_raw_pos, RAW_CUSTOMER → frostbyte_raw_customer
  - ALTER WAREHOUSE ... SIZE = XLARGE → not needed; VCluster scales automatically
  - MATCH_BY_COLUMN_NAME = CASE_SENSITIVE → ZettaPark infers schema from Parquet

Run with:
  python steps/02_load_raw.py
"""

import os
import pathlib
import sys

from dotenv import load_dotenv
from clickzetta.zettapark.session import Session

load_dotenv(pathlib.Path(__file__).parent.parent / ".env")

VOL_SCHEMA = "frostbyte_raw_pos"   # Volume lives in frostbyte_raw_pos
VOL_NAME   = os.environ.get("CLICKZETTA_VOLUME", "frostbyte_vol")
VOL_BASE   = f"vol://{VOL_SCHEMA}.{VOL_NAME}"

POS_TABLES = {
    "menu":         f"{VOL_BASE}/pos/menu/",
    "truck":        f"{VOL_BASE}/pos/truck/",
    "country":      f"{VOL_BASE}/pos/country/",
    "franchise":    f"{VOL_BASE}/pos/franchise/",
    "location":     f"{VOL_BASE}/pos/location/",
    # order_header and order_detail: load only year=2021 root files (not year=2022 subdir)
    # Paths are resolved dynamically from local datasets/ directory
    "order_header": None,
    "order_detail": None,
}

CUSTOMER_TABLES = {
    "customer_loyalty": f"{VOL_BASE}/customer/customer_loyalty/",
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


def resolve_vol_path(tname, local_subdir):
    """
    For tables with mixed year=YYYY subdirs, find the root-level Parquet file
    from local datasets/ and return its explicit Volume path.
    """
    datasets = pathlib.Path(__file__).parent.parent / "datasets"
    local_dir = datasets / local_subdir
    files = [f for f in local_dir.glob("*.snappy.parquet") if f.is_file()]
    if not files:
        raise FileNotFoundError(f"No .snappy.parquet files in {local_dir}")
    # Use the first file found (year=2021 data)
    fname = files[0].name
    return f"{VOL_BASE}/{local_subdir}/{fname}"


def load_table(session, schema, tname, vol_path):
    print(f"  Loading {schema}.{tname} from {vol_path}")
    df = session.read.option("compression", "snappy").parquet(vol_path)
    df.write.save_as_table(f"{schema}.{tname}", mode="overwrite")
    count = session.table(f"{schema}.{tname}").count()
    print(f"    → {count:,} rows")


def validate(session):
    print("\nValidation:")
    for tname in list(POS_TABLES.keys()) + list(CUSTOMER_TABLES.keys()):
        schema = "frostbyte_raw_pos" if tname != "customer_loyalty" else "frostbyte_raw_customer"
        try:
            count = session.table(f"{schema}.{tname}").count()
            print(f"  {schema}.{tname}: {count:,} rows")
        except Exception as e:
            print(f"  {schema}.{tname}: ERROR — {e}")


if __name__ == "__main__":
    session = create_session()

    print("Loading POS tables into frostbyte_raw_pos ...")
    for tname, vol_path in POS_TABLES.items():
        if vol_path is None:
            vol_path = resolve_vol_path(tname, f"pos/{tname}")
        load_table(session, "frostbyte_raw_pos", tname, vol_path)

    print("\nLoading customer tables into frostbyte_raw_customer ...")
    for tname, vol_path in CUSTOMER_TABLES.items():
        load_table(session, "frostbyte_raw_customer", tname, vol_path)

    validate(session)
    print("\nDone. Run next step:")
    print("  python steps/04_create_pos_view.py")
