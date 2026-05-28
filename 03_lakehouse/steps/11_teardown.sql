/*-----------------------------------------------------------------------------
Lakehouse Migration: Data Engineering with ZettaPark
Script:       11_teardown.sql
Migrated from: 01_snowflake/steps/11_teardown.sql

Migration notes:
  Snowflake: DROP DATABASE / DROP WAREHOUSE / DROP ROLE
  Lakehouse: DROP SCHEMA (no multi-level database or warehouse to drop)
             Tasks must be taken offline before deletion

Run with:
  cz-cli sql -f steps/11_teardown.sql --profile frostbyte --sync --write
-----------------------------------------------------------------------------*/

-- Drop tasks first (must be offline before deletion)
-- cz-cli task undeploy orders_update_task -y --profile frostbyte
-- cz-cli task delete orders_update_task -y --profile frostbyte
-- cz-cli task undeploy daily_city_metrics_task -y --profile frostbyte
-- cz-cli task delete daily_city_metrics_task -y --profile frostbyte

-- Drop streams
DROP TABLE STREAM IF EXISTS frostbyte_harmonized.orders_stream;
DROP TABLE STREAM IF EXISTS frostbyte_harmonized.pos_flattened_v_stream;

-- Drop analytics objects
DROP FUNCTION IF EXISTS frostbyte_analytics.fahrenheit_to_celsius_udf(DECIMAL(35,4));
DROP FUNCTION IF EXISTS frostbyte_analytics.inch_to_millimeter_udf(DECIMAL(35,4));
DROP TABLE IF EXISTS frostbyte_analytics.daily_city_metrics;

-- Drop harmonized objects
DROP TABLE IF EXISTS frostbyte_harmonized.orders;
DROP TABLE IF EXISTS frostbyte_harmonized.pos_flattened_v_table;
DROP VIEW  IF EXISTS frostbyte_harmonized.pos_flattened_v;

-- Drop raw tables
DROP TABLE IF EXISTS frostbyte_raw_pos.order_header;
DROP TABLE IF EXISTS frostbyte_raw_pos.order_detail;
DROP TABLE IF EXISTS frostbyte_raw_pos.menu;
DROP TABLE IF EXISTS frostbyte_raw_pos.truck;
DROP TABLE IF EXISTS frostbyte_raw_pos.location;
DROP TABLE IF EXISTS frostbyte_raw_pos.franchise;
DROP TABLE IF EXISTS frostbyte_raw_pos.country;
DROP TABLE IF EXISTS frostbyte_raw_customer.customer_loyalty;

-- Drop volume (removes all uploaded data files)
DROP VOLUME IF EXISTS public.frostbyte_vol;

-- Drop schemas
DROP SCHEMA IF EXISTS frostbyte_analytics;
DROP SCHEMA IF EXISTS frostbyte_harmonized;
DROP SCHEMA IF EXISTS frostbyte_raw_pos;
DROP SCHEMA IF EXISTS frostbyte_raw_customer;
