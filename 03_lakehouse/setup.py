"""
setup.py — One-shot initialization for the Lakehouse migration project.

What this script does:
  1. Creates schemas (frostbyte_raw, frostbyte_harmonized, frostbyte_analytics)
  2. Creates a Volume for raw Parquet files
  3. Uploads local Frostbyte data files to the Volume
  4. Registers a cz-cli profile so shell scripts can use --profile frostbyte

Usage:
  cp .env.example .env          # fill in your connection details
  pip install clickzetta_zettapark_python python-dotenv
  python setup.py

Data files expected in ./datasets/ (download from S3 first):
  aws s3 sync s3://sfquickstarts/data-engineering-with-snowpark-python/ ./datasets/ \\
    --no-sign-request \\
    --exclude "pos/order_header/*" --exclude "pos/order_detail/*"
  # For order_header and order_detail, download only one year=2021 file each:
  aws s3 cp s3://sfquickstarts/data-engineering-with-snowpark-python/pos/order_header/year=2021/<file>.snappy.parquet \\
    ./datasets/pos/order_header/year=2021/ --no-sign-request
  aws s3 cp s3://sfquickstarts/data-engineering-with-snowpark-python/pos/order_detail/year=2021/<file>.snappy.parquet \\
    ./datasets/pos/order_detail/year=2021/ --no-sign-request
"""

import os
import pathlib
import subprocess
import sys

from dotenv import load_dotenv
from clickzetta.zettapark.session import Session

load_dotenv()

SCHEMAS = ["frostbyte_raw_pos", "frostbyte_raw_customer", "frostbyte_harmonized", "frostbyte_analytics"]
VOL_SCHEMA = "public"   # Volume lives in public schema (always exists)
VOL_NAME   = os.environ.get("CLICKZETTA_VOLUME", "frostbyte_vol")
PROFILE    = os.environ.get("CZ_PROFILE", "frostbyte")
DATASETS   = pathlib.Path(__file__).parent / "datasets"


def create_schemas_and_volume_via_cli():
    """
    Create schemas and volume using cz-cli before starting a ZettaPark session.
    ZettaPark session requires the schema to exist at connection time.
    """
    import subprocess
    profile = os.environ.get("CZ_PROFILE", "frostbyte")
    print("Creating schemas and volume via cz-cli...")
    for schema in SCHEMAS:
        result = subprocess.run(
            ["cz-cli", "sql",
             f"CREATE SCHEMA IF NOT EXISTS {schema}",
             "--profile", profile, "--sync", "--write"],
            capture_output=True, text=True
        )
        status = "OK" if result.returncode == 0 else f"WARNING: {result.stderr.strip()}"
        print(f"  schema {schema}: {status}")

    result = subprocess.run(
        ["cz-cli", "sql",
         f"CREATE VOLUME IF NOT EXISTS {VOL_SCHEMA}.{VOL_NAME}",
         "--profile", profile, "--sync", "--write"],
        capture_output=True, text=True
    )
    status = "OK" if result.returncode == 0 else f"WARNING: {result.stderr.strip()}"
    print(f"  volume {VOL_SCHEMA}.{VOL_NAME}: {status}")


def create_session():
    # Connect using 'public' schema — Volume lives in frostbyte_raw_pos
    # but session schema must exist at connection time
    return Session.builder.configs({
        "username":  os.environ["CLICKZETTA_USERNAME"],
        "password":  os.environ["CLICKZETTA_PASSWORD"],
        "service":   os.environ["CLICKZETTA_SERVICE"],
        "instance":  os.environ["CLICKZETTA_INSTANCE"],
        "workspace": os.environ["CLICKZETTA_WORKSPACE"],
        "schema":    "public",
        "vcluster":  os.environ.get("CLICKZETTA_VCLUSTER", "default"),
    }).create()


def create_schemas(session):
    print("Creating schemas...")
    for schema in SCHEMAS:
        session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}").collect()
        print(f"  {schema} OK")


def create_volume(session):
    # Volume already created via cz-cli in create_schemas_and_volume_via_cli()
    pass


def upload_datasets(session):
    if not DATASETS.exists():
        print(f"ERROR: datasets/ directory not found at {DATASETS}")
        print("  Run the aws s3 commands in the docstring above to download data first.")
        sys.exit(1)

    vol_base = f"vol://{VOL_SCHEMA}.{VOL_NAME}"
    print(f"Uploading datasets to {vol_base} ...")

    for f in sorted(DATASETS.rglob("*.parquet")):
        relative = f.relative_to(DATASETS)
        dest = f"{vol_base}/{relative}"
        print(f"  {relative} → {dest}")
        session.file.put(str(f), dest, auto_compress=False, overwrite=True)

    print("  Upload complete.")


def register_cz_profile():
    """Register a cz-cli profile so shell scripts can use --profile frostbyte."""
    print(f"Registering cz-cli profile '{PROFILE}'...")
    cmd = [
        "cz-cli", "profile", "create",
        "--name",      PROFILE,
        "--service",   os.environ["CLICKZETTA_SERVICE"],
        "--instance",  os.environ["CLICKZETTA_INSTANCE"],
        "--workspace", os.environ["CLICKZETTA_WORKSPACE"],
        "--username",  os.environ["CLICKZETTA_USERNAME"],
        "--password",  os.environ["CLICKZETTA_PASSWORD"],
        "--vcluster",  os.environ.get("CLICKZETTA_VCLUSTER", "default"),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  WARNING: cz-cli profile add failed: {result.stderr.strip()}")
        print("  You can register the profile manually:")
        print(f"    cz-cli profile add --name {PROFILE} ...")
    else:
        print(f"  Profile '{PROFILE}' registered.")


if __name__ == "__main__":
    register_cz_profile()
    create_schemas_and_volume_via_cli()  # must run before creating ZettaPark session
    session = create_session()
    upload_datasets(session)
    print("\nSetup complete. You can now run the steps in order:")
    print("  python steps/02_load_raw.py")
    print("  python steps/04_create_pos_view.py")
    print("  python steps/06_orders_update.py")
    print("  python steps/07_daily_city_metrics.py")
    print("  bash steps/08_orchestrate_tasks.sh")
