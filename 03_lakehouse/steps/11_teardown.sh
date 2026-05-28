#!/usr/bin/env bash
# 11_teardown.sh — Clean up all Lakehouse and Studio objects created by this project.
#
# Migrated from: 01_snowflake/steps/11_teardown.sql
#
# Migration notes:
#   Snowflake: DROP DATABASE / DROP WAREHOUSE / DROP ROLE (single SQL script)
#   Lakehouse: Studio tasks must be taken offline via cz-cli BEFORE SQL cleanup;
#              SQL objects (schemas, tables, volume) cleaned up via cz-cli sql
#
# Usage:
#   bash steps/11_teardown.sh --profile frostbyte
#   bash steps/11_teardown.sh  # uses CZ_PROFILE env var or 'frostbyte' default

set -e

PROFILE="${CZ_PROFILE:-frostbyte}"
if [ "$1" = "--profile" ] && [ -n "$2" ]; then
    PROFILE="$2"
fi

echo "Teardown using profile: $PROFILE"
echo ""

# ── Step 1: Take Studio tasks offline and delete ──────────────────────────────
# Snowflake: no equivalent (tasks are SQL objects, dropped with DROP TASK)
# Lakehouse: Studio tasks must be taken offline before deletion
echo "Step 1: Removing Studio tasks..."
for task in orders_update_task daily_city_metrics_task; do
    echo "  Undeploying $task..."
    cz-cli task undeploy "$task" -y --profile "$PROFILE" 2>/dev/null || echo "    (already offline or not found)"
    echo "  Deleting $task..."
    cz-cli task delete "$task" -y --profile "$PROFILE" 2>/dev/null || echo "    (not found)"
done
echo "  Tasks removed."
echo ""

# ── Step 2: Drop SQL objects ──────────────────────────────────────────────────
echo "Step 2: Dropping SQL objects..."
cz-cli sql -f "$(dirname "$0")/11_teardown.sql" --profile "$PROFILE" --sync --write
echo "  SQL objects dropped."
echo ""

echo "Teardown complete."
