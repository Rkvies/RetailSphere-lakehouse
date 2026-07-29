-- Run this FIRST, before anything else, to remove tables created under
-- the old structure (single "lakehouse" schema with bronze_/silver_/
-- gold_ prefixed table names). We're switching to one schema PER
-- layer (bronze, silver, gold, quarantine) with plain table names
-- inside each - cleaner, matches how Unity Catalog is meant to be used,
-- and avoids name collisions if the same business name is meaningful
-- at multiple layers.

-- See what currently exists first:
SHOW TABLES IN retail_lakehouse.lakehouse;

-- Drops the entire old schema and everything in it (all bronze_*,
-- silver_*, gold_* tables). CASCADE is required since the schema
-- contains tables.
DROP SCHEMA IF EXISTS retail_lakehouse.lakehouse CASCADE;

-- Confirm it's gone:
SHOW SCHEMAS IN retail_lakehouse;
