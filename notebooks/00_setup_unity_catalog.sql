-- Run once, in a SQL notebook cell or the SQL editor, before the first
-- pipeline run.

SHOW CATALOGS;

CREATE CATALOG IF NOT EXISTS retail_lakehouse;
USE CATALOG retail_lakehouse;

CREATE SCHEMA IF NOT EXISTS lakehouse;
USE SCHEMA lakehouse;

-- Only the LANDING zone needs a Volume - raw files are unstructured
-- input, which is exactly what Volumes are for. Bronze/Silver/Gold are
-- created as MANAGED tables directly by the pipeline (saveAsTable) -
-- Unity Catalog handles their storage itself, no Volume or explicit
-- LOCATION needed. (An earlier version of this project tried to put
-- Bronze/Silver/Gold under a second Volume with CREATE TABLE ...
-- LOCATION - this fails with "Missing cloud file system scheme"
-- because table LOCATIONs require a registered External Location with
-- cloud storage credentials, which a Volume path doesn't have. Managed
-- tables sidestep this entirely and are the more idiomatic UC pattern.)
CREATE VOLUME IF NOT EXISTS landing;

-- Verify:
LIST '/Volumes/retail_lakehouse/lakehouse/landing';
