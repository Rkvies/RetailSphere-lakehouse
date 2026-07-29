-- Run once (after 00a_cleanup_old_schema.sql if migrating from the old
-- structure), in a SQL cell or the SQL editor.
--
-- Structure: bronze/silver/gold/quarantine are each their OWN schema,
-- containing plain-named tables (bronze.sales, silver.dim_customer,
-- gold.fact_sales, etc.) - not one shared schema with prefixed names.
-- This is the idiomatic Unity Catalog pattern and is what lets you
-- browse Catalog Explorer by layer directly.

SHOW CATALOGS;

CREATE CATALOG IF NOT EXISTS retail_lakehouse;
USE CATALOG retail_lakehouse;

CREATE SCHEMA IF NOT EXISTS landing;   -- holds the raw-files Volume only
CREATE SCHEMA IF NOT EXISTS bronze;    -- managed tables
CREATE SCHEMA IF NOT EXISTS silver;    -- managed tables
CREATE SCHEMA IF NOT EXISTS gold;      -- managed tables
CREATE SCHEMA IF NOT EXISTS quarantine; -- managed tables

-- Only landing needs a Volume - raw files are unstructured input, which
-- is what Volumes are for. Bronze/Silver/Gold/Quarantine tables are
-- MANAGED tables (created via saveAsTable in the pipeline code) - no
-- Volume or LOCATION needed for them; Unity Catalog handles their
-- storage directly.
CREATE VOLUME IF NOT EXISTS landing.files;

-- Verify:
LIST '/Volumes/retail_lakehouse/landing/files';
SHOW SCHEMAS IN retail_lakehouse;
