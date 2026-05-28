/*-----------------------------------------------------------------------------
Lakehouse Migration: Data Engineering with ZettaPark
Script:       05_udf.sql
Migrated from: 01_snowflake/steps/05_fahrenheit_to_celsius_udf/

Migration notes:
  Snowflake: Python UDF deployed via SnowCLI
      - function.py uses scipy.constants.convert_temperature
      - Deployed as a Snowpark Python UDF running inside Snowflake
      - Requires Anaconda integration and third-party package approval

  Lakehouse: SQL UDF (recommended) or External Function (Python UDF)
      - Python UDFs in Lakehouse require External Function service setup
        (cloud function deployment on Alibaba Cloud FC / Tencent SCF / AWS Lambda)
      - For simple math functions like unit conversion, SQL UDF is equivalent
        and requires no additional infrastructure
      - SQL UDF syntax is identical to Snowflake SQL UDF syntax

  FAHRENHEIT_TO_CELSIUS: Python UDF → SQL UDF
      Original Python: return convert_temperature(float(temp_f), 'F', 'C')
      SQL equivalent:  (temp_f - 32) * (5.0 / 9)

  INCH_TO_MILLIMETER: already a SQL UDF in original project (no change needed)

Run with:
  cz-cli sql -f steps/05_udf.sql --profile frostbyte --sync --write
-----------------------------------------------------------------------------*/

-- FAHRENHEIT_TO_CELSIUS_UDF
-- Snowflake: Python UDF (scipy.constants.convert_temperature), body in $$ ... $$
-- Lakehouse: SQL UDF, body uses RETURN expr (no $$ delimiter needed)
-- Note: use DOUBLE (not DECIMAL) — DECIMAL multiplication overflows in UDF context
CREATE OR REPLACE FUNCTION frostbyte_analytics.fahrenheit_to_celsius_udf(temp_f DOUBLE)
RETURNS DOUBLE
RETURN (temp_f - 32.0) * 5.0 / 9.0;

-- INCH_TO_MILLIMETER_UDF
-- Snowflake: SQL UDF with $$ ... $$ — Lakehouse uses RETURN expr
CREATE OR REPLACE FUNCTION frostbyte_analytics.inch_to_millimeter_udf(inch DOUBLE)
RETURNS DOUBLE
RETURN inch * 25.4;

-- Verify (actual output from cz-cli)
SELECT
    frostbyte_analytics.fahrenheit_to_celsius_udf(32)   AS freezing_c,   -- 0
    frostbyte_analytics.fahrenheit_to_celsius_udf(212)  AS boiling_c,    -- 100
    frostbyte_analytics.fahrenheit_to_celsius_udf(98.6) AS body_temp_c,  -- 37
    frostbyte_analytics.inch_to_millimeter_udf(1)       AS one_inch_mm,  -- 25.4
    frostbyte_analytics.inch_to_millimeter_udf(12)      AS one_foot_mm;  -- 304.8
