#!/usr/bin/env bash
# 08_orchestrate_tasks.sh — Schedule pipeline steps as cz-cli tasks.
#
# Migrated from: 01_snowflake/steps/08_orchestrate_jobs.sql
#
# Migration notes:
#   Snowflake: CREATE TASK ... WAREHOUSE = HOL_WH
#              WHEN SYSTEM$STREAM_HAS_DATA('POS_FLATTENED_V_STREAM')
#              AS CALL HARMONIZED.ORDERS_UPDATE_SP()
#   Lakehouse: cz-cli task create <name> --type PYTHON
#              + cz-cli task save-content <name> --content <inline script>
#              + cz-cli task save-cron <name> --cron "*/5 * * * *"
#              + cz-cli task deploy <name> -y
#              + cz-cli task execute <name>
#
#   Key differences:
#   - No SYSTEM$STREAM_HAS_DATA trigger → fixed cron schedule
#   - No stored procedure → Python script content saved directly into task
#   - Studio task runs in isolated env; connection info embedded in script
#   - Task dependency (AFTER ...) → offset cron schedules
#
# Prerequisites:
#   1. source .env before running this script
#   2. Run setup.py first to register the 'frostbyte' cz-cli profile
#
# Usage:
#   export $(grep -v '^#' .env | xargs) && bash steps/08_orchestrate_tasks.sh

set -e

PROFILE="${CZ_PROFILE:-frostbyte}"

echo "Using cz-cli profile: $PROFILE"

# Generate inline task scripts with connection info embedded
# (Studio tasks run in isolated env without local .env)
ORDERS_SCRIPT=$(cat <<PYEOF
import os
from clickzetta.zettapark.session import Session

session = Session.builder.configs({
    "username":  "${CLICKZETTA_USERNAME}",
    "password":  "${CLICKZETTA_PASSWORD}",
    "service":   "${CLICKZETTA_SERVICE}",
    "instance":  "${CLICKZETTA_INSTANCE}",
    "workspace": "${CLICKZETTA_WORKSPACE}",
    "schema":    "frostbyte_harmonized",
    "vcluster":  "${CLICKZETTA_VCLUSTER}",
}).create()

stream_count = session.table("frostbyte_harmonized.pos_flattened_v_stream").count()
orders_count = session.table("frostbyte_harmonized.orders").count()
print(f"{stream_count} records in stream, {orders_count} in orders")

if stream_count == 0 and orders_count == 0:
    # Initial load from view
    session.sql("""
        INSERT INTO frostbyte_harmonized.orders
        SELECT *, CURRENT_TIMESTAMP() AS meta_updated_at
        FROM frostbyte_harmonized.pos_flattened_v
    """).collect()
elif stream_count == 0:
    print("No new data, skipping.")
    exit(0)
else:
    # Incremental load from stream (stream has rsuffix column names from pos_flattened_v_table)
    session.sql("""
        INSERT INTO frostbyte_harmonized.orders
        SELECT
            order_detail_id, order_id, truck_id, menu_item_id, line_number,
            order_ts_oh AS order_ts, quantity, unit_price, price,
            order_amount, order_tax_amount, order_discount_amount, order_total,
            location_id, primary_city, region_l AS region, iso_country_code_l AS iso_country_code,
            country_l AS country, truck_id_t, menu_type_id, truck_city,
            truck_brand_name, menu_type, menu_item_name, item_category, item_subcategory,
            cost_of_goods_usd, sale_price_usd, franchise_id_f AS franchise_id,
            franchise_flag, franchisee_first_name, franchisee_last_name, order_ts_date,
            CURRENT_TIMESTAMP() AS meta_updated_at
        FROM frostbyte_harmonized.pos_flattened_v_stream
    """).collect()

count = session.table("frostbyte_harmonized.orders").count()
print(f"Done. orders: {count} rows")
PYEOF
)

METRICS_SCRIPT=$(cat <<PYEOF
import os
from clickzetta.zettapark.session import Session
from clickzetta.zettapark import functions as F
import clickzetta.zettapark.types as T

session = Session.builder.configs({
    "username":  "${CLICKZETTA_USERNAME}",
    "password":  "${CLICKZETTA_PASSWORD}",
    "service":   "${CLICKZETTA_SERVICE}",
    "instance":  "${CLICKZETTA_INSTANCE}",
    "workspace": "${CLICKZETTA_WORKSPACE}",
    "schema":    "frostbyte_analytics",
    "vcluster":  "${CLICKZETTA_VCLUSTER}",
}).create()

stream_count = session.table("frostbyte_harmonized.orders_stream").count()
print(f"{stream_count} records in orders_stream")
if stream_count == 0:
    print("No new data, skipping.")
    exit(0)

orders = (
    session.table("frostbyte_harmonized.orders_stream")
    .group_by(F.col("order_ts_date"), F.col("primary_city"), F.col("country"))
    .agg(F.sum(F.col("price")).alias("price_sum"))
    .with_column("daily_sales", F.coalesce(F.col("price_sum"), F.lit(0)))
    .select(F.col("order_ts_date").alias("date"),
            F.col("primary_city").alias("city_name"),
            F.col("country").alias("country_desc"),
            F.col("daily_sales"))
)
for col in ["avg_temperature_fahrenheit","avg_temperature_celsius",
            "avg_precipitation_inches","avg_precipitation_millimeters","max_wind_speed_100m_mph"]:
    orders = orders.with_column(col, F.lit(None).cast(T.DecimalType(18,4)))

target = session.table("frostbyte_analytics.daily_city_metrics")
updates = {c: orders[c] for c in orders.schema.names}
updates["meta_updated_at"] = F.current_timestamp()
target.merge(orders,
    (target["date"]==orders["date"]) & (target["city_name"]==orders["city_name"]) & (target["country_desc"]==orders["country_desc"]),
    [F.when_matched().update(updates), F.when_not_matched().insert(updates)])
print(f"Done. daily_city_metrics: {target.count()} rows")
PYEOF
)

# ── Task 1: orders_update_task (every 5 minutes) ──────────────────────────────
echo "Creating task: orders_update_task..."
cz-cli task create orders_update_task --type PYTHON --profile "$PROFILE"
cz-cli task save-content orders_update_task --content "$ORDERS_SCRIPT" --profile "$PROFILE"
cz-cli task save-cron orders_update_task --cron "*/5 * * * *" --profile "$PROFILE"
cz-cli task deploy orders_update_task -y --profile "$PROFILE"
echo "  orders_update_task deployed."

# ── Task 2: daily_city_metrics_task (every 10 minutes) ────────────────────────
echo "Creating task: daily_city_metrics_task..."
cz-cli task create daily_city_metrics_task --type PYTHON --profile "$PROFILE"
cz-cli task save-content daily_city_metrics_task --content "$METRICS_SCRIPT" --profile "$PROFILE"
cz-cli task save-cron daily_city_metrics_task --cron "*/10 * * * *" --profile "$PROFILE"
cz-cli task deploy daily_city_metrics_task -y --profile "$PROFILE"
echo "  daily_city_metrics_task deployed."

# ── Trigger immediate first run ───────────────────────────────────────────────
echo "Triggering immediate run of orders_update_task..."
cz-cli task execute orders_update_task --profile "$PROFILE"

echo ""
echo "Tasks deployed. Monitor with:"
echo "  cz-cli task list --profile $PROFILE"
