/*-----------------------------------------------------------------------------
Lakehouse Migration: Data Engineering with ZettaPark
Script:       01_setup_lakehouse.sql
Migrated from: 01_snowflake/steps/01_setup_snowflake.sql

Run with:
  cz-cli sql -f steps/01_setup_lakehouse.sql --profile frostbyte --sync --write

Migration notes:
  USE ROLE ACCOUNTADMIN         → USE ROLE instance_admin
      Lakehouse uses instance_admin as the top-level admin role.
  SET MY_USER = CURRENT_USER()  → not needed; GRANT TO USER uses literal name
  CREATE DATABASE               → not applicable; Lakehouse uses schemas directly
  CREATE WAREHOUSE              → not applicable; compute is managed via VCluster
  GRANT EXECUTE TASK ON ACCOUNT → not needed; task execution is via cz-cli
  GRANT IMPORTED PRIVILEGES     → not applicable
  CREATE STAGE (S3)             → CREATE VOLUME
      Snowflake STAGE points to an external S3 URL; Lakehouse VOLUME is managed
      internal storage. Data is uploaded via session.file.put() in setup.py.
  FILE FORMAT PARQUET           → not needed; ZettaPark reads Parquet natively
  SQL UDF syntax                → identical; CREATE FUNCTION ... AS $$ ... $$
-----------------------------------------------------------------------------*/

-- ----------------------------------------------------------------------------
-- Step 1: Switch to admin role
-- ----------------------------------------------------------------------------
USE ROLE instance_admin;

-- ----------------------------------------------------------------------------
-- Step 2: Create schemas
-- Snowflake: CREATE DATABASE HOL_DB + multiple schemas inside it
-- Lakehouse: schemas are top-level objects (no multi-level database)
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS frostbyte_raw_pos;
CREATE SCHEMA IF NOT EXISTS frostbyte_raw_customer;
CREATE SCHEMA IF NOT EXISTS frostbyte_harmonized;
CREATE SCHEMA IF NOT EXISTS frostbyte_analytics;

-- ----------------------------------------------------------------------------
-- Step 3: Create Volume (replaces Snowflake STAGE)
-- Snowflake: STAGE points to s3://sfquickstarts/data-engineering-with-snowpark-python/
-- Lakehouse: VOLUME is managed internal storage; data uploaded via setup.py
-- ----------------------------------------------------------------------------
CREATE VOLUME IF NOT EXISTS frostbyte_raw_pos.frostbyte_vol;

-- ----------------------------------------------------------------------------
-- Step 4: Create SQL UDF
-- Snowflake: CREATE FUNCTION ... AS $$ ... $$
-- Lakehouse: use RETURN expr syntax (no $$ delimiter); use DOUBLE to avoid
--   DECIMAL precision overflow in multiplication. See 05_udf.sql for details.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION frostbyte_analytics.fahrenheit_to_celsius_udf(temp_f DOUBLE)
RETURNS DOUBLE
RETURN (temp_f - 32.0) * 5.0 / 9.0;

CREATE OR REPLACE FUNCTION frostbyte_analytics.inch_to_millimeter_udf(inch DOUBLE)
RETURNS DOUBLE
RETURN inch * 25.4;

-- ----------------------------------------------------------------------------
-- Step 5: Grant usage to current user (optional, for multi-user setups)
-- Snowflake: GRANT OWNERSHIP ON DATABASE / WAREHOUSE TO ROLE HOL_ROLE
-- Lakehouse: GRANT USAGE ON SCHEMA ... TO USER ...
-- ----------------------------------------------------------------------------
-- GRANT USAGE ON SCHEMA frostbyte_raw_pos TO USER <your_username>;
-- GRANT USAGE ON SCHEMA frostbyte_harmonized TO USER <your_username>;
-- GRANT USAGE ON SCHEMA frostbyte_analytics TO USER <your_username>;
