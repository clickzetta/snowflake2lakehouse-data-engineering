"""
e2e.py — End-to-end pipeline runner and data validator
        for snowflake2lakehouse-data-engineering (Frostbyte)

Runs the full pipeline from scratch and validates results against
expected values derived from the actual Lakehouse execution.

Usage:
    cd 03_lakehouse
    python e2e.py                  # run pipeline + validate
    python e2e.py --skip-pipeline  # validate only (pipeline already ran)
    python e2e.py --teardown       # run pipeline + validate + teardown
"""

import os
import sys
import subprocess
import pathlib
from dotenv import load_dotenv
from clickzetta.zettapark.session import Session

load_dotenv(pathlib.Path(__file__).parent / ".env")

SKIP_PIPELINE = "--skip-pipeline" in sys.argv
DO_TEARDOWN   = "--teardown"      in sys.argv
PROFILE       = os.environ.get("CZ_PROFILE", "frostbyte")
STEPS_DIR     = pathlib.Path(__file__).parent / "steps"

# ── Ground truth (from actual Lakehouse execution, year=2021 data) ────────────
EXPECTED = {
    # Raw tables (year=2021 data only)
    "raw_menu_rows":             100,
    "raw_truck_rows":            450,
    "raw_country_rows":          30,
    "raw_franchise_rows":        335,
    "raw_location_rows":         13093,
    "raw_order_header_rows":     7336341,
    "raw_order_detail_rows":     6230167,
    "raw_customer_loyalty_rows": 222540,

    # pos_flattened_v (5-table JOIN)
    "view_rows":                 378941,
    "view_distinct_brands":      15,
    "view_distinct_cities":      6,
    "view_distinct_menu_items":  58,

    # orders (harmonized)
    "orders_rows":               378941,
    "orders_null_pk":            0,          # no NULL order_detail_id
    "orders_top_brand":          "Freezing Point",
    "orders_top_brand_count":    42309,
    "orders_total_revenue":      5547817.75,
    "orders_min_date":           "2021-01-01",
    "orders_max_date":           "2022-01-01",

    # daily_city_metrics (analytics)
    "metrics_rows":              247,
    "metrics_top_city":          "New York City",
    "metrics_top_city_sales":    2231534.00,

    # UDFs
    "udf_boiling_c":             100.0,      # fahrenheit_to_celsius(212)
    "udf_one_inch_mm":           25.4,       # inch_to_millimeter(1)
}

PASS = 0
FAIL = 0


def check(label, actual, expected, *, tolerance=0):
    global PASS, FAIL
    if tolerance:
        ok = abs(float(actual) - float(expected)) <= tolerance
    else:
        ok = actual == expected
    if ok:
        PASS += 1
        print(f"  [PASS] {label}: {actual}")
    else:
        FAIL += 1
        print(f"  [FAIL] {label}: got {actual!r}, expected {expected!r}")


def make_session(schema="frostbyte_harmonized"):
    return Session.builder.configs({
        "username":  os.environ["CLICKZETTA_USERNAME"],
        "password":  os.environ["CLICKZETTA_PASSWORD"],
        "service":   os.environ["CLICKZETTA_SERVICE"],
        "instance":  os.environ["CLICKZETTA_INSTANCE"],
        "workspace": os.environ["CLICKZETTA_WORKSPACE"],
        "schema":    schema,
        "vcluster":  os.environ.get("CLICKZETTA_VCLUSTER", "default"),
    }).create()


def run_step(label, cmd, **kwargs):
    print(f"\n  Running: {label}")
    # Pass current environment explicitly so subprocess inherits all env vars
    # (needed for cz-cli and ZettaPark to find credentials)
    result = subprocess.run(cmd, **kwargs, capture_output=True, text=True,
                            env=os.environ.copy())
    # filter noisy retry lines
    for line in result.stdout.splitlines():
        if not any(x in line for x in ["Submit returned", "Execute sql job", "execute_with_retrying"]):
            print(f"    {line}")
    if result.returncode != 0:
        print(f"  [ERROR] {label} failed:\n{result.stderr[-500:]}")
        sys.exit(1)


def run_pipeline():
    print("\n" + "="*60)
    print("PIPELINE — running all steps")
    print("="*60)

    run_step("setup.py", ["python", "setup.py"], cwd=pathlib.Path(__file__).parent)
    run_step("02_load_raw.py", ["python", "steps/02_load_raw.py"], cwd=pathlib.Path(__file__).parent)
    run_step("04_create_pos_view.py", ["python", "steps/04_create_pos_view.py"], cwd=pathlib.Path(__file__).parent)
    run_step("05_udf.sql", ["cz-cli", "sql", "-f", "steps/05_udf.sql",
                            "--profile", PROFILE, "--sync", "--write"],
             cwd=pathlib.Path(__file__).parent)
    run_step("06_orders_update.py", ["python", "steps/06_orders_update.py"], cwd=pathlib.Path(__file__).parent)
    run_step("07_daily_city_metrics.py", ["python", "steps/07_daily_city_metrics.py"], cwd=pathlib.Path(__file__).parent)

    print("\n  Pipeline complete.")


def validate():
    print("\n" + "="*60)
    print("VALIDATION — checking data quality")
    print("="*60)

    session_raw  = make_session("frostbyte_raw_pos")
    session_harm = make_session("frostbyte_harmonized")
    session_ana  = make_session("frostbyte_analytics")

    def sql(session, q):
        return session.sql(q).collect()

    # ── Raw tables ────────────────────────────────────────────────────────────
    print("\n[1] Raw tables")
    for tname, key in [
        ("menu",             "raw_menu_rows"),
        ("truck",            "raw_truck_rows"),
        ("country",          "raw_country_rows"),
        ("franchise",        "raw_franchise_rows"),
        ("location",         "raw_location_rows"),
        ("order_header",     "raw_order_header_rows"),
        ("order_detail",     "raw_order_detail_rows"),
    ]:
        n = sql(session_raw, f"SELECT COUNT(*) AS n FROM frostbyte_raw_pos.{tname}")[0]["n"]
        check(f"frostbyte_raw_pos.{tname} rows", n, EXPECTED[key])

    session_cust = make_session("frostbyte_raw_customer")
    n = sql(session_cust, "SELECT COUNT(*) AS n FROM frostbyte_raw_customer.customer_loyalty")[0]["n"]
    check("frostbyte_raw_customer.customer_loyalty rows", n, EXPECTED["raw_customer_loyalty_rows"])

    # ── pos_flattened_v ───────────────────────────────────────────────────────
    print("\n[2] pos_flattened_v (5-table JOIN)")
    n = sql(session_harm, "SELECT COUNT(*) AS n FROM frostbyte_harmonized.pos_flattened_v")[0]["n"]
    check("pos_flattened_v rows", n, EXPECTED["view_rows"])

    row = sql(session_harm, "SELECT COUNT(DISTINCT truck_brand_name) AS n FROM frostbyte_harmonized.pos_flattened_v")[0]
    check("distinct truck brands", row["n"], EXPECTED["view_distinct_brands"])

    row = sql(session_harm, "SELECT COUNT(DISTINCT primary_city) AS n FROM frostbyte_harmonized.pos_flattened_v")[0]
    check("distinct cities", row["n"], EXPECTED["view_distinct_cities"])

    row = sql(session_harm, "SELECT COUNT(DISTINCT menu_item_name) AS n FROM frostbyte_harmonized.pos_flattened_v")[0]
    check("distinct menu items", row["n"], EXPECTED["view_distinct_menu_items"])

    # ── orders ────────────────────────────────────────────────────────────────
    print("\n[3] frostbyte_harmonized.orders")
    n = sql(session_harm, "SELECT COUNT(*) AS n FROM frostbyte_harmonized.orders")[0]["n"]
    check("orders rows", n, EXPECTED["orders_rows"])

    null_pk = sql(session_harm, "SELECT COUNT(*) AS n FROM frostbyte_harmonized.orders WHERE order_detail_id IS NULL")[0]["n"]
    check("orders NULL primary key", null_pk, EXPECTED["orders_null_pk"])

    row = sql(session_harm, """
        SELECT truck_brand_name, COUNT(*) AS orders
        FROM frostbyte_harmonized.orders
        GROUP BY truck_brand_name ORDER BY orders DESC LIMIT 1
    """)[0]
    check("orders top brand name",  row["truck_brand_name"], EXPECTED["orders_top_brand"])
    check("orders top brand count", row["orders"],           EXPECTED["orders_top_brand_count"])

    revenue = sql(session_harm, "SELECT ROUND(SUM(price), 2) AS total FROM frostbyte_harmonized.orders")[0]["total"]
    check("orders total revenue", float(revenue), EXPECTED["orders_total_revenue"], tolerance=1.0)

    row = sql(session_harm, "SELECT MIN(order_ts_date) AS min_d, MAX(order_ts_date) AS max_d FROM frostbyte_harmonized.orders")[0]
    check("orders min date", str(row["min_d"]), EXPECTED["orders_min_date"])
    check("orders max date", str(row["max_d"]), EXPECTED["orders_max_date"])

    # ── daily_city_metrics ────────────────────────────────────────────────────
    print("\n[4] frostbyte_analytics.daily_city_metrics")
    n = sql(session_ana, "SELECT COUNT(*) AS n FROM frostbyte_analytics.daily_city_metrics")[0]["n"]
    check("daily_city_metrics rows", n, EXPECTED["metrics_rows"])

    row = sql(session_ana, """
        SELECT city_name, ROUND(SUM(daily_sales), 2) AS total
        FROM frostbyte_analytics.daily_city_metrics
        GROUP BY city_name ORDER BY total DESC LIMIT 1
    """)[0]
    check("top city name",  row["city_name"], EXPECTED["metrics_top_city"])
    check("top city sales", float(row["total"]), EXPECTED["metrics_top_city_sales"], tolerance=1.0)

    # ── UDFs ──────────────────────────────────────────────────────────────────
    print("\n[5] SQL UDFs")
    row = sql(session_ana, """
        SELECT frostbyte_analytics.fahrenheit_to_celsius_udf(212) AS boiling,
               frostbyte_analytics.inch_to_millimeter_udf(1)      AS one_inch
    """)[0]
    check("fahrenheit_to_celsius(212)", float(row["boiling"]),  EXPECTED["udf_boiling_c"])
    check("inch_to_millimeter(1)",      float(row["one_inch"]), EXPECTED["udf_one_inch_mm"])

    # ── Summary ───────────────────────────────────────────────────────────────
    total = PASS + FAIL
    print(f"\n{'='*60}")
    print(f"  Result: {PASS}/{total} passed")
    if FAIL > 0:
        print(f"  {FAIL} check(s) FAILED — see above")
        return False
    else:
        print("  All checks passed ✓")
        return True


def teardown():
    print("\n" + "="*60)
    print("TEARDOWN — cleaning up all objects")
    print("="*60)
    run_step("11_teardown.sh", ["bash", "steps/11_teardown.sh", "--profile", PROFILE],
             cwd=pathlib.Path(__file__).parent)


def main():
    print("="*60)
    print("snowflake2lakehouse-data-engineering  E2E")
    print(f"  Profile: {PROFILE}")
    print(f"  Skip pipeline: {SKIP_PIPELINE}")
    print(f"  Teardown after: {DO_TEARDOWN}")
    print("="*60)

    if not SKIP_PIPELINE:
        run_pipeline()

    ok = validate()

    if DO_TEARDOWN:
        teardown()

    if not ok:
        sys.exit(1)

    print("\nE2E complete.")


if __name__ == "__main__":
    main()
