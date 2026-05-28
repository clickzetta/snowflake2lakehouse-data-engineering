#!/usr/bin/env bash
# 08_orchestrate_tasks.sh — Schedule pipeline steps as cz-cli tasks.
#
# Migrated from: 01_snowflake/steps/08_orchestrate_jobs.sql
#
# Migration notes:
#   Snowflake: CREATE TASK ... WAREHOUSE = HOL_WH
#              WHEN SYSTEM$STREAM_HAS_DATA('POS_FLATTENED_V_STREAM')
#              AS CALL HARMONIZED.ORDERS_UPDATE_SP()
#   Lakehouse: cz-cli task create --command "python ..." --schedule "*/5 * * * *"
#       - No SYSTEM$STREAM_HAS_DATA trigger; use fixed cron schedule instead
#       - No stored procedure; call Python scripts directly
#       - Task dependency (AFTER ORDERS_UPDATE_TASK) → offset cron schedules
#         (orders_update every 5 min, daily_city_metrics every 10 min)
#
# Prerequisites:
#   1. Run setup.py first to register the 'frostbyte' cz-cli profile
#   2. Ensure .env is populated with connection details
#
# Usage:
#   bash steps/08_orchestrate_tasks.sh

set -e

PROFILE="${CZ_PROFILE:-frostbyte}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Using cz-cli profile: $PROFILE"

# Step 1: Create task for orders_update (every 5 minutes)
# Snowflake: triggered by SYSTEM$STREAM_HAS_DATA('POS_FLATTENED_V_STREAM')
# Lakehouse: fixed schedule — runs every 5 minutes, script checks stream internally
echo "Creating task: orders_update_task (every 5 min)..."
cz-cli task create \
  --name orders_update_task \
  --command "python ${SCRIPT_DIR}/06_orders_update.py" \
  --schedule "*/5 * * * *" \
  --profile "$PROFILE"

# Step 2: Create task for daily_city_metrics (every 10 minutes)
# Snowflake: AFTER ORDERS_UPDATE_TASK (dependency chain)
# Lakehouse: offset by 5 minutes to run after orders_update completes
echo "Creating task: daily_city_metrics_task (every 10 min)..."
cz-cli task create \
  --name daily_city_metrics_task \
  --command "python ${SCRIPT_DIR}/07_daily_city_metrics.py" \
  --schedule "*/10 * * * *" \
  --profile "$PROFILE"

# Step 3: Trigger immediate first run (equivalent to EXECUTE TASK in Snowflake)
echo "Triggering immediate run of orders_update_task..."
cz-cli task run --name orders_update_task --profile "$PROFILE"

echo ""
echo "Tasks created. Monitor with:"
echo "  cz-cli task list --profile $PROFILE"
echo "  cz-cli task logs --name orders_update_task --profile $PROFILE"
